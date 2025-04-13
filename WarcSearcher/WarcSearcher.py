import atexit
import configparser
import datetime
import glob
import os
import re
import shutil
import sys
import threading
import time
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor,
                                as_completed, wait)
from multiprocessing import Manager

import psutil
from fastwarc.stream_io import FileStream, GZipStream
from fastwarc.warc import ArchiveIterator

from validators import *
from helpers import *
from logger import *

WARC_GZ_ARCHIVES_DIRECTORY = ''
SEARCH_QUERIES_DIRECTORY = ''
RESULTS_OUTPUT_DIRECTORY = ''
RESULTS_OUTPUT_SUBDIRECTORY = ''
ZIP_FILES_WITH_MATCHES = False
MAX_ARCHIVE_READ_THREADS = None
MAX_SEARCH_PROCESSES = None
TARGET_PROCESS_MEMORY = None

#MAX_RECURSION_DEPTH = 50
REGEX_PATTERNS_LIST = []
TXT_FILES_DICT = {}
SEARCH_QUEUE = None

def begin_search():
    manager = Manager()

    txt_locks, definitions = setup_txt_locks(manager)

    global SEARCH_QUEUE
    SEARCH_QUEUE = manager.Queue()
    with ProcessPoolExecutor(max_workers=MAX_SEARCH_PROCESSES-1) as executor:
        futures = [executor.submit(find_and_write_matches_subprocess, 
                                   SEARCH_QUEUE, 
                                   definitions, 
                                   txt_locks, 
                                   ZIP_FILES_WITH_MATCHES) for _ in range(MAX_SEARCH_PROCESSES-1)]

        iterate_through_gz_files(WARC_GZ_ARCHIVES_DIRECTORY)

        for _ in range(MAX_SEARCH_PROCESSES):
            SEARCH_QUEUE.put(None)

        WarcSearcherLogger.log_info("Waiting on search processes to finish - This may take a while, please wait...")

        # With no more records to read from the WARCs, put the main process to work with searching and monitor the queue on a background thread
        stop_event = threading.Event()
        monitoring_thread = threading.Thread(target=monitor_remaining_queue_items, args=(SEARCH_QUEUE, stop_event))
        monitoring_thread.start()

        find_and_write_matches_subprocess(SEARCH_QUEUE, definitions, txt_locks, ZIP_FILES_WITH_MATCHES)
        wait(futures)

        stop_event.set()
        monitoring_thread.join()

    if ZIP_FILES_WITH_MATCHES:
        WarcSearcherLogger.log_info("Finalizing the zip archives...")
        tempdir = os.path.join(RESULTS_OUTPUT_SUBDIRECTORY, "temp")
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(merge_zip_files, 
                                       tempdir,
                                       RESULTS_OUTPUT_SUBDIRECTORY, 
                                       os.path.basename(os.path.splitext(txt_path)[0])): txt_path for txt_path, _ in definitions}
            for future in as_completed(futures):
                future.result()

        shutil.rmtree(tempdir)


def setup_txt_locks(manager):
    txt_locks = manager.dict()
    definitions = list(zip(TXT_FILES_DICT, REGEX_PATTERNS_LIST))

    for txt_path, regex in definitions:
        with open(txt_path, "a", encoding='utf-8') as output_file:
            initialize_txt_output_file(output_file, txt_path, regex)
        txt_locks[txt_path] = manager.Lock()
    return txt_locks,definitions


def iterate_through_gz_files(gz_directory_path):
    gz_files = glob.glob(f"{gz_directory_path}/**/*.gz", recursive=True)

    if not gz_files:
        WarcSearcherLogger.log_error(f"No .gz files were found at the root or any subdirectories of: {gz_directory_path}")
        sys.exit()

    with ThreadPoolExecutor(max_workers=MAX_ARCHIVE_READ_THREADS) as executor:
        tasks = {executor.submit(open_warc_gz_file, gz_file_path) for gz_file_path in gz_files}

        for future in as_completed(tasks):
            future.result()


def open_warc_gz_file(gz_file_path):
    gz_file_stream = GZipStream(FileStream(gz_file_path, 'rb'))
    WarcSearcherLogger.log_info(f"Beginning to process {gz_file_path}")

    try:
        records = ArchiveIterator(gz_file_stream, strict_mode=False)
        if not any(records):
            WarcSearcherLogger.log_warning(f"No WARC records found in {gz_file_path}")
            return

        records_searched = 0
        for record in records:
            if record.headers['WARC-Type'] == 'response':
                records_searched += 1
                record_content = record.reader.read()
                record_name = record.headers['WARC-Target-URI']
                search_function(record_content, record_name, gz_file_path)

                if records_searched % 1000 == 0:
                    WarcSearcherLogger.log_info(f"Read {records_searched} response records from the WARC in {gz_file_path}")
                    process = psutil.Process()
                    while get_total_memory_usage(process) > TARGET_PROCESS_MEMORY:
                        WarcSearcherLogger.log_warning(f"Process memory is beyond target size specified in config.ini. Will attempt to continue after 10 seconds to allow time to process the existing queue...")
                        time.sleep(10)
    except Exception as e:
        WarcSearcherLogger.log_error(f"Error ocurred when reading contents of {gz_file_path}: \n{e}")


def search_function(file_data, file_name, root_gz_file):
    record_obj = RecordData(root_gz_file=root_gz_file, name=file_name, contents=file_data)
    SEARCH_QUEUE.put(record_obj)


def get_definition_files():
    """
    Find all definition files in the search queries directory.
    
    Returns:
        list: A list of paths to definition files
    """
    return glob.glob(os.path.join(SEARCH_QUERIES_DIRECTORY, '*.txt'))


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
            WarcSearcherLogger.log_error(f"Invalid regular expression found in {definition_file}")
            return None, False
            
    except IOError as e:
        WarcSearcherLogger.log_error(f"Error reading file {definition_file}: {str(e)}")
        return None, False


def create_output_file_path(definition_file):
    """
    Create an output file path for the findings based on the definition file name.
    
    Args:
        definition_file (str): Path to the definition file
        
    Returns:
        str: Path to the output file
    """
    filename_without_extension = os.path.splitext(os.path.basename(definition_file))[0]
    output_filename = f"{filename_without_extension}_findings.txt"
    return os.path.join(RESULTS_OUTPUT_SUBDIRECTORY, output_filename)


def create_regex_and_output_txt_file_collections():
    """
    Create regex patterns from definition files and prepare output file paths.
    
    This function:
    1. Reads regex patterns from text files in the SEARCH_QUERIES_DIRECTORY
    2. Compiles valid patterns and adds them to REGEX_PATTERNS_LIST
    3. Creates corresponding output file paths in TXT_FILES_DICT
    4. Exits if no valid regex patterns are found
    """

    definition_files = get_definition_files()
    
    for definition_file in definition_files:
        regex_pattern, success = compile_regex_pattern(definition_file)
        
        if success:
            REGEX_PATTERNS_LIST.append(regex_pattern)
            
            output_filepath = create_output_file_path(definition_file)
            TXT_FILES_DICT[output_filepath] = output_filepath

    validate_regex_patterns(REGEX_PATTERNS_LIST)


def create_results_output_subdirectory():
    """Creates a timestamped subdirectory in the results output directory to store the search results for the current execution."""

    results_subdirectory_name = "WarcSearcher_Results_" + datetime.datetime.now().strftime('%m-%d-%y_%H_%M_%S')
    
    global RESULTS_OUTPUT_SUBDIRECTORY
    RESULTS_OUTPUT_SUBDIRECTORY = os.path.join(RESULTS_OUTPUT_DIRECTORY, results_subdirectory_name)
    os.makedirs(RESULTS_OUTPUT_SUBDIRECTORY)

    WarcSearcherLogger.log_info(f"Results output directory created in: {RESULTS_OUTPUT_DIRECTORY}")

    if ZIP_FILES_WITH_MATCHES:
        os.makedirs(os.path.join(RESULTS_OUTPUT_SUBDIRECTORY, "temp"))


def read_config_ini_variables():
    """Reads the variables found in ther config.ini file after ensuring it exists."""

    if not os.path.isfile('config.ini'):
        WarcSearcherLogger.log_error("config.ini file does not exist in the working directory.")
        sys.exit()

    parser = configparser.ConfigParser()
    parser.read('config.ini')

    try:
        read_required_config_ini_variables(parser)
        read_optional_config_ini_variables(parser)
        
    except Exception as e:
        WarcSearcherLogger.log_error(f"Error reading the contents of the config.ini file: \n{e}")
        sys.exit()


def read_required_config_ini_variables(parser):
    """Reads the required variables from the config.ini file, validates them, and sets them as global variables."""

    global WARC_GZ_ARCHIVES_DIRECTORY
    WARC_GZ_ARCHIVES_DIRECTORY = parser.get('REQUIRED', 'warc_gz_archives_directory')
    validate_warc_gz_archives_directory(WARC_GZ_ARCHIVES_DIRECTORY)

    global SEARCH_QUERIES_DIRECTORY
    SEARCH_QUERIES_DIRECTORY = parser.get('REQUIRED', 'search_queries_directory')
    validate_search_queries_directory(SEARCH_QUERIES_DIRECTORY)
        
    global RESULTS_OUTPUT_DIRECTORY
    RESULTS_OUTPUT_DIRECTORY = parser.get('REQUIRED', 'results_output_directory')
    validate_results_output_directory(RESULTS_OUTPUT_DIRECTORY)

def read_optional_config_ini_variables(parser):
    """Reads the optional variables from the config.ini file and sets them as global variables."""

    global ZIP_FILES_WITH_MATCHES
    ZIP_FILES_WITH_MATCHES = parser.getboolean('OPTIONAL', 'zip_files_with_matches')

    # TODO maybe remove this and just use the default value of 4 for max_archive_read_threads .
    global MAX_ARCHIVE_READ_THREADS
    threads_item = parser.get('OPTIONAL', 'max_concurrent_archive_read_threads').lower()
    MAX_ARCHIVE_READ_THREADS = min(32, os.cpu_count() + 4) if threads_item == "none" else int(threads_item)

    global MAX_SEARCH_PROCESSES
    processes_item = parser.get('OPTIONAL', 'max_concurrent_search_processes').lower()
    MAX_SEARCH_PROCESSES = os.cpu_count() if processes_item == "none" else int(processes_item)

    global TARGET_PROCESS_MEMORY
    process_memory_item = parser.get('OPTIONAL', 'target_process_memory_bytes').lower()
    TARGET_PROCESS_MEMORY = 32000000000 if process_memory_item == "none" else int(process_memory_item)


def move_log_file_to_results_subdirectory():
    """Moves the log file to the results output subdirectory, or keeps it in the working directory if an output subdirectory was not created."""

    if RESULTS_OUTPUT_SUBDIRECTORY != '':
        working_directory_log_path = os.path.join(os.getcwd(), 'output_log.log')
        results_output_subdirectory_log_path = os.path.join(RESULTS_OUTPUT_SUBDIRECTORY, 'output_log.log')
        shutil.move(working_directory_log_path, results_output_subdirectory_log_path)
    else:
        # Keep log file in the working directory if no results subdirectory was created as part of the execution
        WarcSearcherLogger.log_info(f"Log file output to working directory: {os.getcwd()}\\output_log")


def finish():
    """
    Function to be called on program exit. Closes the logging file handler, moves reports errors/warnings.
    
    Args:
        logging_handler: The logging file handler.
    """

    WarcSearcherLogger.report_errors_and_warnings()
    WarcSearcherLogger.close_logging_file_handler()
    move_log_file_to_results_subdirectory()


if __name__ == '__main__':
    # Store the start time
    start_time = time.time()

    # Initialize logging, create a log file in the working directory
    logging_handler = WarcSearcherLogger.initialize_logging_to_file()

    # Register the finish function to be automatically called on program exit
    atexit.register(lambda: finish())

    # Read the config.ini file variables and store them as global variables
    read_config_ini_variables()

    # Create the results subdirectory in the output folder
    create_results_output_subdirectory()

    # Create the regex and output txt file collections
    create_regex_and_output_txt_file_collections()

    # Start the search
    begin_search()

    if RESULTS_OUTPUT_SUBDIRECTORY != '':
        WarcSearcherLogger.log_info(f"Results output to: {RESULTS_OUTPUT_SUBDIRECTORY}")

    elapsedMinutes, elapsedSeconds = calculate_execution_time(start_time)
    WarcSearcherLogger.log_info(f"Finished searching. Elapsed time: {elapsedMinutes}m {elapsedSeconds}s")