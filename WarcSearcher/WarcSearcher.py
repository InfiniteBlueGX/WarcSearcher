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
from termcolor import colored
from warcio.archiveiterator import ArchiveIterator

ARCHIVES_DIRECTORY = ''
DEFINITIONS_DIRECTORY = ''
FINDINGS_OUTPUT_PATH = ''
ZIP_FILES_WITH_MATCHES = False
OUTPUT_TXT_FILES_LIST = []
REGEX_LIST = []
PATTERNS_LIST = []
MAX_RECURSION_DEPTH = 50
MAX_THREADS = 4
ZIP_LOCKS = defaultdict(Lock)

logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )


def iterate_through_gz_files(gz_directory_path):
    gz_files = glob.glob(f"{gz_directory_path}/**/*.gz", recursive=True)

    if not gz_files:
        logging.warning(colored(f"No .gz files were found in {gz_directory_path} or any subdirectories.", 'yellow'))
        return

    with ThreadPoolExecutor(MAX_THREADS) as executor:
        tasks = {executor.submit(open_warc_gz_file, gz_file_path) for gz_file_path in gz_files}

        for future in as_completed(tasks):
            future.result()

                
def open_warc_gz_file(gz_file_path):
    with gzip.open(gz_file_path, 'rb') as warc_gz_file:
        logging.info(colored(f"Beginning to process {gz_file_path}", 'blue'))

        if not contains_warc_file(warc_gz_file):
            logging.warning(colored(f"Cannot read contents of {warc_gz_file.name} - Either the .gz archive does not contain a WARC file, or the WARC file is malformed.", 'yellow'))
            return       

        try:
            for index, record in enumerate(ArchiveIterator(warc_gz_file)):
                if record.rec_type == 'response':
                    file_content = record.content_stream().read()
                    file_name = record.rec_headers.get_header('WARC-Target-URI')
                    search_function(file_content, file_name, gz_file_path, 0)
                    
                # Every 200 records processed from the WARC file, log the total number of records searched to keep track
                if index > 0 and index % 200 == 0:
                    logging.info(f"Processed {index} records in {gz_file_path}")
        except Exception as e:
            logging.error(colored(f"Error ocurred when reading contents of {gz_file_path}: \n{e}", 'red'))


def search_function(file_data, searched_file_name, root_gz_file, recursion_depth):
    if recursion_depth == MAX_RECURSION_DEPTH:
        logging.error(colored(f"Error: Maximum recursion depth of {MAX_RECURSION_DEPTH} was hit - terminating to avoid infinite looping.", 'red'))
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
        with rarfile.RarFile(BytesIO(file_data)) as rawr_file:
            for file_name in rawr_file.infolist():
                with rawr_file.open(file_name, mode='r') as nested_file:
                    search_function(nested_file.read(), nested_file.name, root_gz_file, recursion_depth)

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
    for pattern, output_file in zip(PATTERNS_LIST, OUTPUT_TXT_FILES_LIST):
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
            findings_txt_file.write(f'[File: {searched_file_name}]\n')
            if searching_name_only:
                write_matches(findings_txt_file, filtered_matches_name, unique_matches_set_name, 'file name')
            else:
                if filtered_matches_name:
                    write_matches(findings_txt_file, filtered_matches_name, unique_matches_set_name, 'file name')
                write_matches(findings_txt_file, filtered_matches_contents, unique_matches_set_contents, 'file contents')
            findings_txt_file.write('___________________________________________________________________\n\n')
    except Exception as e:
        logging.error(colored(f"Error ocurred when writing matches to findings file: {searched_file_name} \n{str(e)}", 'red'))


def filter_and_extract_unique(matches):
    filtered_matches = [match.group() for match in matches]
    unique_matches_set = list(set(filtered_matches))
    return filtered_matches, unique_matches_set


def write_matches(findings_txt_file, filtered_matches, unique_matches_set, match_type):
    findings_txt_file.write(f'[Matches found in {match_type}: {len(filtered_matches)} ({len(filtered_matches)-len(unique_matches_set)} duplicates omitted)]\n')
    for match_counter, match in enumerate(unique_matches_set, start=1):
        findings_txt_file.write(f'\n[Match #{match_counter} in {match_type}]\n\n"{match}"\n\n')


def write_file_with_match_to_zip(file_data, searched_file_name, output_file):
    try:
        full_zip_path = os.path.join(FINDINGS_OUTPUT_PATH, (f"{os.path.splitext(output_file)[0]}.zip"))
    
        with ZIP_LOCKS[full_zip_path]:
            with zipfile.ZipFile(full_zip_path, 'r') as zip_file_read:
                if reformat_file_name(searched_file_name) in zip_file_read.namelist():
                    return

            with zipfile.ZipFile(full_zip_path, 'a', zipfile.ZIP_DEFLATED) as zip_output_file:
                searched_file_name_reformatted = reformat_file_name(searched_file_name)
                zip_output_file.writestr(searched_file_name_reformatted, file_data)

    except Exception as e:
        logging.error(colored(f"Error ocurred when appending zip archive with file: {searched_file_name} \n{str(e)}", 'red'))


def reformat_file_name(file_name):
    web_prefixes_removed = re.sub(r'(http://|https://|www.)', '', file_name)
    return re.sub(r'[\\/*?:"<>|]', '_', web_prefixes_removed)
    

def contains_warc_file(warc_gz_file):
    first_bytes = warc_gz_file.read(10)
    warc_gz_file.seek(0)  # reset the file's internal pointer to the beginning
    return first_bytes.startswith(b'WARC/') 


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
            REGEX_LIST.append(df.read().strip())
            OUTPUT_TXT_FILES_LIST.append(f"{os.path.splitext(os.path.basename(definition_file))[0]}_findings.txt")


def create_patterns():
    global PATTERNS_LIST 
    PATTERNS_LIST = [re.compile(regex, re.IGNORECASE) for regex in REGEX_LIST]


def initialize_output_data():
    for regex, output_txt_file in zip(REGEX_LIST, OUTPUT_TXT_FILES_LIST):
        full_txt_path = os.path.join(FINDINGS_OUTPUT_PATH, output_txt_file)
        with open(full_txt_path, 'a', encoding='utf-8') as findings_txt_file:
            timestamp = datetime.datetime.now().strftime('%Y.%m.%d %H:%M:%S')
            findings_txt_file.write(f'[{output_txt_file}]\n')
            findings_txt_file.write(f'[Created: {timestamp}]\n\n')
            findings_txt_file.write(f'[Regex used]\n{regex}\n\n')
            findings_txt_file.write('___________________________________________________________________\n\n')
        if ZIP_FILES_WITH_MATCHES:
            full_zip_path = os.path.join(FINDINGS_OUTPUT_PATH, (f"{os.path.splitext(output_txt_file)[0]}.zip"))
            with zipfile.ZipFile(full_zip_path, 'w') as empty_zip:
                pass


def create_output_directory():
    findings_directory = "Findings_" + datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S')
    global FINDINGS_OUTPUT_PATH
    if not os.path.exists(FINDINGS_OUTPUT_PATH):
        logging.warning(colored(f"Output path does not exist - using current working directory instead: {os.getcwd()}", 'yellow'))
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


def check_arguments():
    if len(sys.argv) > 1:
        if sys.argv[1] == 'zip':
            global ZIP_FILES_WITH_MATCHES
            ZIP_FILES_WITH_MATCHES = True


def valid_input_directories():
    if not os.path.exists(ARCHIVES_DIRECTORY):
        logging.error(colored(f"Directory containing the .gz archives to search does not exist: {ARCHIVES_DIRECTORY}", 'red'))
        return False
    if not os.path.exists(DEFINITIONS_DIRECTORY):
        logging.error(colored(f"Directory containing the regex definition files does not exist: {DEFINITIONS_DIRECTORY}", 'red'))
        return False
    
    return True


def read_globals_from_config():   

    if not os.path.isfile('config.ini'):
        logging.error(colored("config.ini file does not exist in the working directory.", 'red'))
        return False

    parser = configparser.ConfigParser()
    parser.read('config.ini')

    global ARCHIVES_DIRECTORY
    ARCHIVES_DIRECTORY = parser.get('GLOBALS', 'archives_directory')

    global DEFINITIONS_DIRECTORY
    DEFINITIONS_DIRECTORY = parser.get('GLOBALS', 'definitions_directory')

    global FINDINGS_OUTPUT_PATH
    FINDINGS_OUTPUT_PATH = parser.get('GLOBALS', 'findings_output_path')

    global ZIP_FILES_WITH_MATCHES
    ZIP_FILES_WITH_MATCHES = parser.getboolean('GLOBALS', 'zip_files_with_matches')

    global MAX_THREADS
    MAX_THREADS = parser.getint('GLOBALS', 'max_threads')

    return True


if __name__ == '__main__':
    if not read_globals_from_config() or not valid_input_directories():
        sys.exit()
    check_arguments()
    create_output_directory()
    initialize_logging_to_file(FINDINGS_OUTPUT_PATH)
    logging.info(f"Findings output directory created: '{FINDINGS_OUTPUT_PATH}'")
    create_regex_and_output_file_lists()
    create_patterns()
    initialize_output_data()
    iterate_through_gz_files(ARCHIVES_DIRECTORY)
    logging.info(colored(f"Finished - results have been output to {FINDINGS_OUTPUT_PATH}", 'light_green'))
