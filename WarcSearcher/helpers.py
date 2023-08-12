import logging
import sys
import zipfile
from io import BytesIO

import py7zr
import rarfile

ERROR_COUNT = 0
WARNING_COUNT = 0


def is_file_binary(file_data):
    # Set of characters typically found in text files
    text_chars = bytearray({7, 8, 9, 10, 12, 13, 27} | set(range(0x20, 0x100)) - {0x7f})
    first_1024_chars = file_data[:1024]
    return bool(first_1024_chars.translate(None, text_chars))


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
    

def reformat_file_name(file_name):
    web_prefixes_removed = file_name.replace('http://', '').replace('https://', '').replace('www.', '')
    return web_prefixes_removed.translate(str.maketrans('','','\\/*?:"<>|'))


def filter_and_extract_unique(matches):
    filtered_matches_list = [match.group() for match in matches]
    unique_matches_set = [match for match in set(filtered_matches_list)]
    return filtered_matches_list, unique_matches_set


def write_matches(output_txt_file, filtered_matches, unique_matches_set, match_type):
    output_txt_file.write(f'[Matches found in {match_type}: {len(filtered_matches)} ({len(filtered_matches)-len(unique_matches_set)} duplicates omitted)]\n')
    for match_counter, match in enumerate(unique_matches_set, start=1):
        output_txt_file.write(f'[Match #{match_counter} in {match_type}]\n\n"{match}"\n\n')


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


def report_errors_and_warnings():
    logging.info(f"[Errors: {ERROR_COUNT}, Warnings: {WARNING_COUNT}]")