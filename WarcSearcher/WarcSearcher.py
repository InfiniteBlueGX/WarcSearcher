import atexit
import configparser
import datetime
import glob
import gzip
import logging
import os
import re
import sys
import threading
import time
import zipfile
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor,
                                as_completed, wait)
from io import BytesIO
from multiprocessing import Manager

import psutil
import py7zr
import rarfile
from fastwarc.stream_io import FileStream, GZipStream
from fastwarc.warc import ArchiveIterator

from helpers import *

ARCHIVES_DIRECTORY = ''
DEFINITIONS_DIRECTORY = ''
FINDINGS_OUTPUT_PATH = ''
ZIP_FILES_WITH_MATCHES = False
REGEX_PATTERNS_LIST = []
MAX_RECURSION_DEPTH = 50
MAX_ARCHIVE_READ_THREADS = None
MAX_SEARCH_PROCESSES = None

TXT_FILES_DICT = {}
#ZIP_FILES_DICT = {}
SEARCH_QUEUE = None
items = 0

def begin_search():
    manager = Manager()

    txt_locks = manager.dict()
    definitions = list(zip(TXT_FILES_DICT, REGEX_PATTERNS_LIST))

    for txt_path, regex in definitions:
        with open(txt_path, "a", encoding='utf-8') as output_file:
            initialize_txt_output_file(output_file, txt_path, regex)
        txt_locks[txt_path] = manager.Lock()

    global SEARCH_QUEUE
    SEARCH_QUEUE = manager.Queue()
    with ProcessPoolExecutor(max_workers=MAX_SEARCH_PROCESSES) as executor:
        futures = [executor.submit(find_and_write_matches_subprocess, SEARCH_QUEUE, definitions, txt_locks) for _ in range(4)]

        iterate_through_gz_files(ARCHIVES_DIRECTORY)

        for _ in range(4):
            SEARCH_QUEUE.put(None)

        logging.info("Waiting on subprocesses to finish searching - This may take a while, please wait...")

        stop_event = threading.Event()
        monitoring_thread = threading.Thread(target=monitor_remaining_queue_items, args=(SEARCH_QUEUE, stop_event))
        monitoring_thread.start()

        wait(futures)

        stop_event.set()
        monitoring_thread.join()
        
    global items
    print(f"Total items - {items}")


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
    process = psutil.Process()
    while get_total_memory_usage(process) > 1000000000:
        log_warning(f"Process memory usage is above maximum. Will attempt to read the next WARC after 30 seconds to allow time to process the existing queue...")
        time.sleep(30)

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
                search_function(record_content, record_name, gz_file_path, 0)

                if records_searched % 200 == 0:
                    logging.info(f"Read {records_searched} response records from the WARC in {gz_file_path}")
    except Exception as e:
        log_error(f"Error ocurred when reading contents of {gz_file_path}: \n{e}")


def search_function(file_data, file_name, root_gz_file, recursion_depth):
    if recursion_depth == MAX_RECURSION_DEPTH:
        log_error(f"Error: Maximum recursion depth of {MAX_RECURSION_DEPTH} was hit - terminating to avoid infinite looping.")
        sys.exit()

    recursion_depth += 1

    if is_zip_file(file_data):
        with zipfile.ZipFile(BytesIO(file_data), 'r') as zipped_file:
            for file_name in zipped_file.namelist():
                with zipped_file.open(file_name, 'r') as nested_file:
                    search_function(nested_file.read(), file_name, root_gz_file, recursion_depth)

    elif is_7z_file(file_data):
        with py7zr.SevenZipFile(BytesIO(file_data), mode='r') as sevenzip_file:
            archive_contents = sevenzip_file.read()
            for file_name, file_content in archive_contents.items():
                search_function(file_content.read(), file_name, root_gz_file, recursion_depth)

    elif is_rar_file(file_data):
        try:
            with rarfile.RarFile(BytesIO(file_data)) as rawr_file:
                for file_name in rawr_file.infolist():
                    with rawr_file.open(file_name, mode='r') as nested_file:
                        search_function(nested_file.read(), nested_file.name, root_gz_file, recursion_depth)
        except Exception:
            log_error(f"Error processing nested .rar archive '{file_name}' in: {root_gz_file}\n\tWinRar is required to process .rar archives. Ensure that WinRar is installed and the path to the folder containing the WinRar executable is added to your System Path environment variable.")

    elif is_gz_file(file_data, file_name):
        with gzip.open(BytesIO(file_data), 'rb') as nested_file:
            nested_file_name = extract_nested_gz_filename(file_data[:200])
            search_function(nested_file.read(), nested_file_name, root_gz_file, recursion_depth)

    elif is_file_binary(file_data):
        # If the file is binary data (image, video, audio, etc), only search the file name, since searching the binary data is wasted effort
        record_obj = RecordData(root_gz_file=root_gz_file, name=file_name, contents=None)
        global SEARCH_QUEUE
        SEARCH_QUEUE.put(record_obj)
        global items
        items += 1

    else:
        record_obj = RecordData(root_gz_file=root_gz_file, name=file_name, contents=file_data)
        SEARCH_QUEUE.put(record_obj)
        items += 1


def create_regex_and_output_txt_file_collections():
    definition_files = glob.glob(DEFINITIONS_DIRECTORY + '/*.txt')
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
        full_txt_path = os.path.join(FINDINGS_OUTPUT_PATH, output_txt_file)
        TXT_FILES_DICT[full_txt_path] = full_txt_path

    if not REGEX_PATTERNS_LIST:
        log_error("There are no valid regular expressions in any of the definition files - terminating execution.")
        sys.exit()


def create_output_directory():
    findings_directory = "Findings_" + datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S')
    global FINDINGS_OUTPUT_PATH
    if not os.path.exists(FINDINGS_OUTPUT_PATH):
        log_warning(f"Output path designated in config.ini does not exist - using current working directory instead: {os.getcwd()}")
        FINDINGS_OUTPUT_PATH = os.path.join(os.getcwd(), findings_directory)
    else:
        FINDINGS_OUTPUT_PATH = os.path.join(FINDINGS_OUTPUT_PATH, findings_directory)
    os.makedirs(FINDINGS_OUTPUT_PATH)


def read_arguments():
    if len(sys.argv) > 1:
        if sys.argv[1] == 'zip':
            global ZIP_FILES_WITH_MATCHES
            ZIP_FILES_WITH_MATCHES = True


def validate_input_directories():
    if not os.path.exists(ARCHIVES_DIRECTORY):
        log_error(f"Directory containing the .gz archives to search does not exist: {ARCHIVES_DIRECTORY}")
        sys.exit()
    if not os.path.exists(DEFINITIONS_DIRECTORY):
        log_error(f"Directory containing the regex definition .txt files does not exist: {DEFINITIONS_DIRECTORY}")
        sys.exit()
    if not glob.glob(DEFINITIONS_DIRECTORY + '/*.txt'):
        log_error(f"Directory that should contain the regex definition .txt files does not contain any: {DEFINITIONS_DIRECTORY}")
        sys.exit()


def read_globals_from_config():
    if not os.path.isfile('config.ini'):
        log_error("config.ini file does not exist in the working directory.")
        sys.exit()

    parser = configparser.ConfigParser()
    parser.read('config.ini')

    try:
        global ARCHIVES_DIRECTORY
        ARCHIVES_DIRECTORY = parser.get('REQUIRED', 'archives_directory')

        global DEFINITIONS_DIRECTORY
        DEFINITIONS_DIRECTORY = parser.get('REQUIRED', 'definitions_directory')

        global FINDINGS_OUTPUT_PATH
        FINDINGS_OUTPUT_PATH = parser.get('OPTIONAL', 'findings_output_path')

        global ZIP_FILES_WITH_MATCHES
        ZIP_FILES_WITH_MATCHES = parser.getboolean('OPTIONAL', 'zip_files_with_matches')

        global MAX_ARCHIVE_READ_THREADS
        threads_item = parser.get('OPTIONAL', 'max_archive_read_threads').lower()
        MAX_ARCHIVE_READ_THREADS = min(32, os.cpu_count() + 4) if threads_item == "none" else int(threads_item)

        global MAX_SEARCH_PROCESSES
        processes_item = parser.get('OPTIONAL', 'max_search_processes').lower()
        MAX_SEARCH_PROCESSES = os.cpu_count() if processes_item == "none" else int(processes_item)
        
    except Exception as e:
        log_error(f"Error reading the contents of the config.ini file: \n{e}")
        sys.exit()


def finish():
    # close_zip_files()
    report_errors_and_warnings()
    input("Press Enter to exit...")


if __name__ == '__main__':
    start_time = time.time()
    atexit.register(finish)
    read_globals_from_config()
    validate_input_directories()
    read_arguments()
    create_output_directory()
    initialize_logging_to_file(FINDINGS_OUTPUT_PATH)
    logging.info(f"Findings output directory created: {FINDINGS_OUTPUT_PATH}")
    create_regex_and_output_txt_file_collections()

    begin_search()
    
    execution_time = time.time() - start_time
    minutes, seconds = divmod(execution_time, 60)
    logging.info(f"Finished in {int(minutes)}m {round(seconds, 2)}s - results output to {FINDINGS_OUTPUT_PATH}")