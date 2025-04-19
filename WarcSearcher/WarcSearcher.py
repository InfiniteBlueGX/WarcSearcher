import atexit
import glob
import os
import re
import shutil
import time
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor,
                                as_completed, wait)
from multiprocessing import Manager

import config
import fileops
import logger
import psutil
from config import *
from fastwarc.stream_io import FileStream, GZipStream
from fastwarc.warc import ArchiveIterator
from fileops import *
from helpers import *
from validators import *

SEARCH_QUEUE = None

def begin_search(definitions_list):
    manager = Manager()

    txt_locks = setup_txt_locks(manager, definitions_list)

    global SEARCH_QUEUE
    SEARCH_QUEUE = manager.Queue()
    with ProcessPoolExecutor(max_workers = config.settings["MAX_SEARCH_PROCESSES"]-1) as executor:
        futures = [executor.submit(find_and_write_matches_subprocess, 
                                   SEARCH_QUEUE, 
                                   definitions_list, 
                                   txt_locks, 
                                   config.settings["ZIP_FILES_WITH_MATCHES"]) for _ in range(config.settings["MAX_SEARCH_PROCESSES"]-1)]

        iterate_through_gz_files(config.settings["WARC_GZ_ARCHIVES_DIRECTORY"])

        # Put a None object in the queue for each process to signal them to stop searching
        for _ in range(config.settings["MAX_SEARCH_PROCESSES"]):
            SEARCH_QUEUE.put(None)

        logger.log_info("Waiting on search processes to finish - This may take a while, please wait...")

        # With no more records to read from the WARCs, put the main process to work with searching and monitor the queue on a background thread
        # stop_event = threading.Event()
        # monitoring_thread = threading.Thread(target=monitor_remaining_queue_items, args=(SEARCH_QUEUE, stop_event))
        # monitoring_thread.start()

        # find_and_write_matches_subprocess(SEARCH_QUEUE, definitions_list, txt_locks, config.settings["ZIP_FILES_WITH_MATCHES"])
        # wait(futures)

        # stop_event.set()
        # monitoring_thread.join()
        wait(futures)

    if config.settings["ZIP_FILES_WITH_MATCHES"]:
        logger.log_info("Finalizing the zip archives...")
        tempdir = os.path.join(fileops.results_output_subdirectory, "temp")
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(merge_zip_files, 
                                       tempdir,
                                       fileops.results_output_subdirectory, 
                                       os.path.basename(os.path.splitext(txt_path)[0])): txt_path for txt_path, _ in definitions_list}
            for future in as_completed(futures):
                future.result()

        shutil.rmtree(tempdir)



def iterate_through_gz_files(gz_directory_path):
    gz_files = glob.glob(f"{gz_directory_path}/**/*.gz", recursive=True)

    validate_gz_file_existence(gz_directory_path, gz_files)

    # Set up 4 threads to read the gz files concurrently - one thread per gz file. 
    # Each thread will open a gz file and put records into the search queue.
    # TODO experiment with different values with different warc files
    with ThreadPoolExecutor(max_workers = 4) as executor:
        tasks = {executor.submit(open_warc_gz_file, gz_file_path) for gz_file_path in gz_files}

        for future in as_completed(tasks):
            future.result()



def open_warc_gz_file(gz_file_path):
    gz_file_stream = GZipStream(FileStream(gz_file_path, 'rb'))
    logger.log_info(f"Beginning to process {gz_file_path}")

    try:
        records = ArchiveIterator(gz_file_stream, strict_mode=False)
        if not any(records):
            logger.log_warning(f"No WARC records found in {gz_file_path}")
            return

        records_searched = 0
        for record in records:
            if record.headers['WARC-Type'] == 'response':
                records_searched += 1
                record_content = record.reader.read()
                record_name = record.headers['WARC-Target-URI']
                record_obj = RecordData(root_gz_file=gz_file_path, name=record_name, contents=record_content)
                SEARCH_QUEUE.put(record_obj)

                if records_searched % 1000 == 0:
                    logger.log_info(f"Read {records_searched} response records from the WARC in {gz_file_path}")
                    process = psutil.Process()
                    while get_total_memory_usage(process) > config.settings["TARGET_PROCESS_MEMORY"]:
                        logger.log_warning(f"Process memory is beyond target size specified in config.ini. Will attempt to continue after 10 seconds to allow time to process the existing queue...")
                        time.sleep(10)
    except Exception as e:
        logger.log_error(f"Error ocurred when reading contents of {gz_file_path}: \n{e}")



def compile_regex_pattern(definition_file):
    """
    Read and compile a regex pattern from a definition file.
    
    Args:
        definition_file (str): Path to the definition file
        
    Returns:
        tuple: (compiled_pattern, success_flag)
            - compiled_pattern: The compiled regex pattern or None if compilation failed
            - success_flag: True if compilation was successful, False otherwise
    """
    try:
        with open(definition_file, 'r', encoding='utf-8') as file:
            raw_regex = file.read().strip()
        
        try:
            regex_pattern = re.compile(raw_regex, re.IGNORECASE)
            return regex_pattern, True
        except re.error:
            logger.log_error(f"Invalid regular expression found in {definition_file}")
            return None, False
            
    except IOError as e:
        logger.log_error(f"Error reading file {definition_file}: {str(e)}")
        return None, False



def create_regex_and_result_file_tuple_collection() -> list: 
    """
    
    """

    regex_patterns_list = []
    results_txt_files_dict = {}

    definition_files = get_definition_txt_files_list()
    
    for definition_file in definition_files:
        regex_pattern, success = compile_regex_pattern(definition_file)
        
        if success:
            regex_patterns_list.append(regex_pattern)
            
            output_filepath = get_results_txt_file_path(definition_file)
            results_txt_files_dict[output_filepath] = output_filepath
        else:
            logger.log_warning(f"Regex pattern in {definition_file} was invalid and will not be used to search.")

    verify_regex_patterns_exist(regex_patterns_list)

    return list(zip(results_txt_files_dict, regex_patterns_list))



def finish():
    """
    Function to be called on program exit. Responsible for logging errors and warnings, closing the logging file handler, 
    and moving the log file to the results subdirectory if it was created.
    """
    logger.log_info(logger_state.get_final_report())
    logger_state.close_logging_file_handler()
    move_log_file_to_results_subdirectory()



if __name__ == '__main__':
    # Store the start time
    start_time = time.time()

    # Initialize logging, create a log file in the working directory
    logger_state.initialize_logging_to_file()

    # Register the finish function to be automatically called on program exit
    atexit.register(lambda: finish())

    # Read the config.ini file variables and store them as global variables
    read_config_ini_variables()

    # Create the results subdirectory in the output folder and set the fileops global to it
    fileops.results_output_subdirectory = create_results_output_subdirectory()

    if config.settings["ZIP_FILES_WITH_MATCHES"]:
        create_temp_directory_for_zip(fileops.results_output_subdirectory)

    # Create the definitions list
    definitions = create_regex_and_result_file_tuple_collection()

    # Start the search
    begin_search(definitions)

    if fileops.results_output_subdirectory != '':
        logger.log_info(f"Results output to: {fileops.results_output_subdirectory}")

    elapsedMinutes, elapsedSeconds = calculate_execution_time(start_time)
    logger.log_info(f"Finished searching. Elapsed time: {elapsedMinutes}m {elapsedSeconds}s")