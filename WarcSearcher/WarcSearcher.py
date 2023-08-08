import atexit
import configparser
import datetime
import glob
import gzip
import logging
import os
import re
import sys
import zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from threading import Lock

import py7zr
import rarfile
from fastwarc.stream_io import FileStream, GZipStream
from fastwarc.warc import ArchiveIterator

ARCHIVES_DIRECTORY = ''
DEFINITIONS_DIRECTORY = ''
FINDINGS_OUTPUT_PATH = ''
ZIP_FILES_WITH_MATCHES = False
OUTPUT_TXT_FILES_LIST = []
REGEX_PATTERNS_LIST = []
MAX_RECURSION_DEPTH = 50
MAX_THREADS = 4
ZIP_LOCKS = defaultdict(Lock)
ERROR_COUNT = 0
WARNING_COUNT = 0

logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )


def iterate_through_gz_files(gz_directory_path):
    gz_files = glob.glob(f"{gz_directory_path}/**/*.gz", recursive=True)

    if not gz_files:
        log_error(f"No .gz files were found at the root or any subdirectories of: {gz_directory_path}")
        sys.exit()

    with ThreadPoolExecutor(MAX_THREADS) as executor:
        tasks = {executor.submit(open_warc_gz_file, gz_file_path) for gz_file_path in gz_files}

        for future in as_completed(tasks):
            future.result()


def open_warc_gz_file(gz_file_path):
    gz_file_stream = GZipStream(FileStream(gz_file_path, 'rb'))
    logging.info(f"Beginning to process {gz_file_path}")     

    try:
        records = ArchiveIterator(gz_file_stream)
        if not any(records):
            log_warning(f"No WARC records found in {gz_file_path}")
            return
        
        records_searched = 0
        for record in records:
            if record.headers['WARC-Type'] == 'response':
                records_searched += 1
                file_content = record.reader.read()
                file_name = record.headers['WARC-Target-URI']
                search_function(file_content, file_name, gz_file_path, 0)
                    
                if records_searched > 0 and records_searched % 200 == 0:
                    logging.info(f"Searched {records_searched} records in {gz_file_path}")
    except Exception as e:
        log_error(f"Error ocurred when reading contents of {gz_file_path}: \n{e}")


def search_function(file_data, searched_file_name, root_gz_file, recursion_depth):
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
            log_error(f"Error processing nested .rar archive '{searched_file_name}' in: {root_gz_file}\n\tWinRar is required to process .rar archives. Ensure that WinRar is installed and the path to the folder containing the WinRar executable is added to your System Path environment variable.")

    elif is_gz_file(file_data):
        with gzip.open(BytesIO(file_data), 'rb') as nested_file:
            search_function(nested_file.read(), searched_file_name, root_gz_file, recursion_depth)

    elif is_file_binary(file_data):
        # If the file is binary data (image, video, audio, etc), only search the file name, since searching the binary data is wasted effort
        search_file(file_data, searched_file_name, root_gz_file, True)
        
    else:
        search_file(file_data, searched_file_name, root_gz_file, False)


def is_file_binary(file_data):
    # Set of characters typically found in text files
    text_chars = bytearray({7, 8, 9, 10, 12, 13, 27} | set(range(0x20, 0x100)) - {0x7f})
    first_1024_chars = file_data[:1024]
    return bool(first_1024_chars.translate(None, text_chars))


def search_file(file_data, searched_file_name, root_gz_file, search_name_only):
    for pattern, output_file in zip(REGEX_PATTERNS_LIST, OUTPUT_TXT_FILES_LIST):
        matches_name = list(re.finditer(pattern, searched_file_name))
        if search_name_only:    
            matches_contents = []
        else:
            matches_contents = list(re.finditer(pattern, file_data.decode('utf-8', 'ignore')))

        if matches_name or matches_contents:
            write_matches_to_findings_file(searched_file_name, output_file, search_name_only, root_gz_file, matches_name, matches_contents)
            if ZIP_FILES_WITH_MATCHES:  
                write_file_with_match_to_zip(file_data, searched_file_name, output_file)


def write_matches_to_findings_file(searched_file_name, output_file, searching_name_only, root_gz_file, matches_name, matches_contents):
    try:
        full_txt_path = os.path.join(FINDINGS_OUTPUT_PATH, output_file)
        filtered_matches_name, unique_matches_set_name = filter_and_extract_unique(matches_name)
        filtered_matches_contents, unique_matches_set_contents = filter_and_extract_unique(matches_contents)
        
        with open(full_txt_path, 'a', encoding='utf-8') as findings_txt_file:
            findings_txt_file.write(f'[Archive: {root_gz_file}]\n')
            findings_txt_file.write(f'[File: {searched_file_name}]\n\n')
            if searching_name_only:
                write_matches(findings_txt_file, filtered_matches_name, unique_matches_set_name, 'file name')
            else:
                if filtered_matches_name:
                    write_matches(findings_txt_file, filtered_matches_name, unique_matches_set_name, 'file name')
                write_matches(findings_txt_file, filtered_matches_contents, unique_matches_set_contents, 'file contents')
            findings_txt_file.write('___________________________________________________________________\n\n')
    except Exception as e:
        log_error(f"Error ocurred when writing matches to findings file: {searched_file_name} \n{str(e)}")


def filter_and_extract_unique(matches):
    filtered_matches = [match.group() for match in matches]
    unique_matches_set = list(set(filtered_matches))
    return filtered_matches, unique_matches_set


def write_matches(findings_txt_file, filtered_matches, unique_matches_set, match_type):
    findings_txt_file.write(f'[Matches found in {match_type}: {len(filtered_matches)} ({len(filtered_matches)-len(unique_matches_set)} duplicates omitted)]\n')
    for match_counter, match in enumerate(unique_matches_set, start=1):
        findings_txt_file.write(f'[Match #{match_counter} in {match_type}]\n\n"{match}"\n\n')


def write_file_with_match_to_zip(file_data, searched_file_name, output_file):
    try:
        full_zip_path = os.path.join(FINDINGS_OUTPUT_PATH, (f"{os.path.splitext(output_file)[0]}.zip"))

        with ZIP_LOCKS[full_zip_path]:
            with zipfile.ZipFile(full_zip_path, 'a', zipfile.ZIP_DEFLATED) as zip_output_file:
                file_name_reformatted = reformat_file_name(searched_file_name)
                if file_name_reformatted not in zip_output_file.namelist():
                    zip_output_file.writestr(file_name_reformatted, file_data)
    except Exception as e:
        log_error(f"Error ocurred when appending zip archive with file: {searched_file_name} \n{str(e)}")


def reformat_file_name(file_name):
    web_prefixes_removed = re.sub(r'(http://|https://|www.)', '', file_name)
    return re.sub(r'[\\/*?:"<>|]', '_', web_prefixes_removed)


def is_zip_file(file_data):
    try:
        return zipfile.is_zipfile(BytesIO(file_data))
    except (AttributeError, TypeError):
        return False


def is_7z_file(file_data):
    try:
        return py7zr.is_7zfile(BytesIO(file_data))
    except (AttributeError, TypeError):
        return False

    
def is_rar_file(file_data):
    try:
        return rarfile.is_rarfile(BytesIO(file_data))
    except (AttributeError, TypeError):
        return False

    
def is_gz_file(file_data):
    try:
        # Magic number for .gz file signature - if it matches the first two bytes of the file data, it's a .gz file
        return file_data[:2].hex() == '1f8b'
    except (AttributeError, TypeError):
        return False
    

def create_regex_and_output_file_lists():
    definition_files = [os.path.join(DEFINITIONS_DIRECTORY, f) for f in os.listdir(DEFINITIONS_DIRECTORY) if f.endswith('.txt')]
    for definition_file in definition_files:
        with open(definition_file, 'r', encoding='utf-8') as df:
            raw_regex = df.read().strip()
            try:
                regex_pattern = re.compile(raw_regex, re.IGNORECASE)
                REGEX_PATTERNS_LIST.append(regex_pattern)
            except re.error:
                log_error(f"Invalid regular expression in {definition_file}: {raw_regex}")
                continue
            OUTPUT_TXT_FILES_LIST.append(f"{os.path.splitext(os.path.basename(definition_file))[0]}_findings.txt")
    
    if not OUTPUT_TXT_FILES_LIST:
        log_error("There are no valid regular expressions in any of the definition files - terminating execution.")
        sys.exit()


def initialize_output_data():
    for pattern, output_txt_file in zip(REGEX_PATTERNS_LIST, OUTPUT_TXT_FILES_LIST):
        full_txt_path = os.path.join(FINDINGS_OUTPUT_PATH, output_txt_file)
        with open(full_txt_path, 'a', encoding='utf-8') as findings_txt_file:
            timestamp = datetime.datetime.now().strftime('%Y.%m.%d %H:%M:%S')
            findings_txt_file.write(f'[{output_txt_file}]\n')
            findings_txt_file.write(f'[Created: {timestamp}]\n\n')
            findings_txt_file.write(f'[Regex used]\n{pattern.pattern}\n\n')
            findings_txt_file.write('___________________________________________________________________\n\n')
        if ZIP_FILES_WITH_MATCHES:
            full_zip_path = os.path.join(FINDINGS_OUTPUT_PATH, (f"{os.path.splitext(output_txt_file)[0]}.zip"))
            with zipfile.ZipFile(full_zip_path, 'w') as empty_zip:
                pass


def create_output_directory():
    findings_directory = "Findings_" + datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S')
    global FINDINGS_OUTPUT_PATH
    if not os.path.exists(FINDINGS_OUTPUT_PATH):
        log_warning(f"Output path does not exist - using current working directory instead: {os.getcwd()}")
        FINDINGS_OUTPUT_PATH = os.path.join(os.getcwd(), findings_directory)
    else:
        FINDINGS_OUTPUT_PATH = os.path.join(FINDINGS_OUTPUT_PATH, findings_directory)
    os.makedirs(FINDINGS_OUTPUT_PATH)


def initialize_logging_to_file(output_directory):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(f"{output_directory}/output_log.log"), logging.StreamHandler(sys.stdout)],
        force=True
    )


def log_warning(message):
    logging.warning(f"{message}")
    global WARNING_COUNT
    WARNING_COUNT += 1


def log_error(message):
    logging.error(f"{message}")
    global ERROR_COUNT
    ERROR_COUNT += 1


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

        global MAX_THREADS
        MAX_THREADS = parser.getint('OPTIONAL', 'max_threads')
    except Exception as e:
        log_error(f"Error reading the contents of the config.ini file: \n{e}")
        sys.exit()


def finish():
    logging.info(f"[Errors: {ERROR_COUNT}, Warnings: {WARNING_COUNT}]")
    input("Press Enter to exit...")


if __name__ == '__main__':
    atexit.register(finish)
    read_globals_from_config()
    validate_input_directories()
    read_arguments()
    create_output_directory()
    initialize_logging_to_file(FINDINGS_OUTPUT_PATH)
    logging.info(f"Findings output directory created: {FINDINGS_OUTPUT_PATH}")
    create_regex_and_output_file_lists()
    initialize_output_data()
    iterate_through_gz_files(ARCHIVES_DIRECTORY)
    logging.info(f"Finished - results output to {FINDINGS_OUTPUT_PATH}")
