import datetime
import logging
import os
import sys
import time
import zipfile
from io import BytesIO, StringIO

import py7zr
import rarfile

ERROR_COUNT = 0
WARNING_COUNT = 0


class RecordData:
  def __init__(self, root_gz_file, name, contents):
    self.root_gz_file = root_gz_file
    self.name = name
    self.contents = contents


def find_and_write_matches_subprocess(record_queue, definitions, txt_locks):
    print(f"Starting search process #{os.getpid()}")
    count = 0

    txt_buffers = {}
    for txt_path, regex in definitions:
        txt_buffers[txt_path] = StringIO()

    while True:
        record_obj: RecordData = record_queue.get()
        if record_obj == None:
            for txt_path, regex in definitions:
                buffer_contents = txt_buffers[txt_path].getvalue()
                with txt_locks[txt_path]:
                    with open(txt_path, "a", encoding='utf-8') as output_file:
                        #print(f"Should be writing to {txt_path}")
                        output_file.write(buffer_contents)
            print(f"Ending search process #{os.getpid()} - items processed: {count}")
            break
        count +=1

        for txt_path, regex in definitions:
            matches_name = []
            matches_contents = []

            matches_name = find_regex_matches(record_obj.name, regex)

            if record_obj.contents == None:
                # searching file name only
                matches_contents = ''
            else:    
                matches_contents = find_regex_matches(record_obj.contents.decode('utf-8', 'ignore'), regex)

            if matches_name or matches_contents:
                write_matches_to_txt_output_buffer(txt_buffers[txt_path], matches_name, matches_contents, record_obj.root_gz_file, record_obj.name)


def initialize_txt_output_file(output_file, txt_file_path, regex):
    timestamp = datetime.datetime.now().strftime('%Y.%m.%d %H:%M:%S')
    output_file.write(f'[{os.path.basename(txt_file_path)}]\n')
    output_file.write(f'[Created: {timestamp}]\n\n')
    output_file.write(f'[Regex used]\n{regex.pattern}\n\n')
    output_file.write('___________________________________________________________________\n\n')


def write_matches_to_txt_output_buffer(output_buffer, matches_list_name, matches_list_contents, root_gz_file, containing_file):
    output_buffer.write(f'[Archive: {root_gz_file}]\n')
    output_buffer.write(f'[File: {containing_file}]\n\n')

    write_matches(output_buffer, matches_list_name, 'file name')
    write_matches(output_buffer, matches_list_contents, 'file contents')

    output_buffer.write('___________________________________________________________________\n\n')


def write_matches(output_buffer, matches_list, match_type):
    if matches_list:
        unique_matches_set = [match for match in set(matches_list)]
        output_buffer.write(f'[Matches found in {match_type}: {len(matches_list)} ({len(matches_list)-len(unique_matches_set)} duplicates omitted)]\n')
        for i, match in enumerate(unique_matches_set, start=1):
            output_buffer.write(f'[Match #{i} in {match_type}]\n\n"{match}"\n\n')


def find_regex_matches(input_string, regex_pattern):
    return [match.group() for match in regex_pattern.finditer(input_string)]


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

    
def is_gz_file(file_data, file_name):
    try:
        # Magic number for .gz file signature - if it matches the first three bytes of the file data, it's a .gz file
        return file_data[:3].hex() == '1f8b08' and os.path.splitext(file_name)[1] == '.gz'
    except (AttributeError, TypeError):
        return False
    

def extract_nested_gz_filename(first_gz_file_bytes):
    parts = first_gz_file_bytes.split(b'\x00')
    if len(parts) > 1:
        return parts[1].decode('utf-8')
    else:
        return None


def reformat_file_name(file_name):
    web_prefixes_removed = file_name.replace('http://', '').replace('https://', '').replace('www.', '')
    return web_prefixes_removed.translate(str.maketrans('','','\\/*?:"<>|'))


def get_total_memory_usage(process):
    mem_info = process.memory_info()
    resident_set_size_memory = mem_info.rss

    subprocesses = process.children(recursive=True)
    for subprocess in subprocesses:
        mem_info = subprocess.memory_info()
        resident_set_size_memory += mem_info.rss

    return resident_set_size_memory


def monitor_remaining_queue_items(queue, stop_event):
    while not stop_event.is_set():
        logging.info(f"Remaining items to search: {queue.qsize()}")
        time.sleep(5)


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