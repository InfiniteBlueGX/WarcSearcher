import atexit
import configparser
import datetime
import glob
#import gzip
#import logging
import os
import re
import shutil
import sys
import threading
import time
#import zipfile
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor,
                                as_completed, wait)
#from io import BytesIO
from multiprocessing import Manager

import psutil
# import py7zr
# import rarfile
from fastwarc.stream_io import FileStream, GZipStream
from fastwarc.warc import ArchiveIterator

from helpers import *
from logging_helpers import *

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

        logging.info("Waiting on search processes to finish - This may take a while, please wait...")

        # With no more records to read from the WARCs, put the main process to work with searching and monitor the queue on a background thread
        stop_event = threading.Event()
        monitoring_thread = threading.Thread(target=monitor_remaining_queue_items, args=(SEARCH_QUEUE, stop_event))
        monitoring_thread.start()

        find_and_write_matches_subprocess(SEARCH_QUEUE, definitions, txt_locks, ZIP_FILES_WITH_MATCHES)
        wait(futures)

        stop_event.set()
        monitoring_thread.join()

    if ZIP_FILES_WITH_MATCHES:
        logging.info("Finalizing the zip archives...")
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
        log_error(f"No .gz files were found at the root or any subdirectories of: {gz_directory_path}")
        sys.exit()

    with ThreadPoolExecutor(max_workers=MAX_ARCHIVE_READ_THREADS) as executor:
        tasks = {executor.submit(open_warc_gz_file, gz_file_path) for gz_file_path in gz_files}

        for future in as_completed(tasks):
            future.result()


def open_warc_gz_file(gz_file_path):
    gz_file_stream = GZipStream(FileStream(gz_file_path, 'rb'))
    logging.info(f"Beginning to process {gz_file_path}")

    try:
        records = ArchiveIterator(gz_file_stream, strict_mode=False)
        if not any(records):
            log_warning(f"No WARC records found in {gz_file_path}")
            return

        records_searched = 0
        for record in records:
            if record.headers['WARC-Type'] == 'response':
                records_searched += 1
                record_content = record.reader.read()
                record_name = record.headers['WARC-Target-URI']
                search_function(record_content, record_name, gz_file_path)

                if records_searched % 1000 == 0:
                    logging.info(f"Read {records_searched} response records from the WARC in {gz_file_path}")
                    process = psutil.Process()
                    while get_total_memory_usage(process) > TARGET_PROCESS_MEMORY:
                        log_warning(f"Process memory is beyond target size specified in config.ini. Will attempt to continue after 10 seconds to allow time to process the existing queue...")
                        time.sleep(10)
    except Exception as e:
        log_error(f"Error ocurred when reading contents of {gz_file_path}: \n{e}")


def search_function(file_data, file_name, root_gz_file):
    record_obj = RecordData(root_gz_file=root_gz_file, name=file_name, contents=file_data)
    SEARCH_QUEUE.put(record_obj)


def create_regex_and_output_txt_file_collections():
    definition_files = glob.glob(SEARCH_QUERIES_DIRECTORY + '/*.txt')
    for definition_file in definition_files:
        with open(definition_file, 'r', encoding='utf-8') as df:
            raw_regex = df.read().strip()
            try:
                regex_pattern = re.compile(raw_regex, re.IGNORECASE)
                REGEX_PATTERNS_LIST.append(regex_pattern)
            except re.error:
                log_error(f"Invalid regular expression found in {definition_file}")
                continue
        output_txt_file = f"{os.path.splitext(os.path.basename(definition_file))[0]}_findings.txt"
        full_txt_path = os.path.join(RESULTS_OUTPUT_SUBDIRECTORY, output_txt_file)
        TXT_FILES_DICT[full_txt_path] = full_txt_path

    if not REGEX_PATTERNS_LIST:
        log_error("There are no valid regular expressions in any of the definition files - terminating execution.")
        sys.exit()


def create_results_output_subdirectory():
    """Creates a timestamped subdirectory in the results output directory to store the search results for the current execution."""

    results_subdirectory_name = "WarcSearcher_Results_" + datetime.datetime.now().strftime('%m-%d-%y_%H_%M_%S')
    
    global RESULTS_OUTPUT_SUBDIRECTORY
    RESULTS_OUTPUT_SUBDIRECTORY = os.path.join(RESULTS_OUTPUT_DIRECTORY, results_subdirectory_name)
    os.makedirs(RESULTS_OUTPUT_SUBDIRECTORY)

    logging.info(f"Results output directory created in: {RESULTS_OUTPUT_DIRECTORY}")

    if ZIP_FILES_WITH_MATCHES:
        os.makedirs(os.path.join(RESULTS_OUTPUT_SUBDIRECTORY, "temp"))


def read_config_ini_variables():
    """Reads the variables found in ther config.ini file after ensuring it exists."""

    if not os.path.isfile('config.ini'):
        log_error("config.ini file does not exist in the working directory.")
        sys.exit()

    parser = configparser.ConfigParser()
    parser.read('config.ini')

    try:
        read_required_config_ini_variables(parser)
        read_optional_config_ini_variables(parser)
        
    except Exception as e:
        log_error(f"Error reading the contents of the config.ini file: \n{e}")
        sys.exit()


def read_optional_config_ini_variables(parser):
    """Reads the optional variables from the config.ini file and sets them as global variables."""

    global ZIP_FILES_WITH_MATCHES
    ZIP_FILES_WITH_MATCHES = parser.getboolean('OPTIONAL', 'zip_files_with_matches')

    # TODO maybe remove this and just use the default value of 4 for max_archive_read_threads
    global MAX_ARCHIVE_READ_THREADS
    threads_item = parser.get('OPTIONAL', 'max_concurrent_archive_read_threads').lower()
    MAX_ARCHIVE_READ_THREADS = min(32, os.cpu_count() + 4) if threads_item == "none" else int(threads_item)

    global MAX_SEARCH_PROCESSES
    processes_item = parser.get('OPTIONAL', 'max_concurrent_search_processes').lower()
    MAX_SEARCH_PROCESSES = os.cpu_count() if processes_item == "none" else int(processes_item)

    global TARGET_PROCESS_MEMORY
    process_memory_item = parser.get('OPTIONAL', 'target_process_memory_bytes').lower()
    TARGET_PROCESS_MEMORY = 32000000000 if process_memory_item == "none" else int(process_memory_item)


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


def move_log_file_to_results_subdirectory():
    """Moves the log file to the results output subdirectory, or keeps it in the working directory if an output subdirectory was not created."""

    if RESULTS_OUTPUT_SUBDIRECTORY != '':
        working_directory_log_path = os.path.join(os.getcwd(), 'output_log.log')
        results_output_subdirectory_log_path = os.path.join(RESULTS_OUTPUT_SUBDIRECTORY, 'output_log.log')
        shutil.move(working_directory_log_path, results_output_subdirectory_log_path)
    else:
        # Keep log file in the working directory if no results subdirectory was created as part of the execution
        logging.info(f"Log file output to working directory: {os.getcwd()}\\output_log")


def calculate_execution_time(start_time):
    """
    Calculates and logs the search execution time based on the provided start time.
    
    Args:
        start_time: The time when execution started
    """

    execution_time = time.time() - start_time
    minutes, seconds = divmod(execution_time, 60)

    logging.info(f"Finished searching. Elapsed time: {int(minutes)}m {round(seconds, 2)}s")


def finish(logging_handler):
    """
    Function to be called on program exit. Closes the logging file handler, moves reports errors/warnings.
    
    Args:
        logging_handler: The logging file handler.
    """

    close_logging_file_handler(logging_handler)
    move_log_file_to_results_subdirectory()
    report_errors_and_warnings()
    input("Press Enter to finish...")


if __name__ == '__main__':
    start_time = time.time()

    # Initialize logging, create a log file in the working directory
    logging_handler = initialize_logging_to_file()

    # Register the finish function to be automatically called on program exit
    atexit.register(lambda: finish(logging_handler))

    # Read the config.ini file variables and store them as global variables
    read_config_ini_variables()

    # Create the results subdirectory in the output folder
    create_results_output_subdirectory()

    
    create_regex_and_output_txt_file_collections()

    begin_search()

    calculate_execution_time(start_time)

    if RESULTS_OUTPUT_SUBDIRECTORY != '':
        logging.info(f"Results output to: {RESULTS_OUTPUT_SUBDIRECTORY}")