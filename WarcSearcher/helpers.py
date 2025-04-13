import datetime
import glob
import logging
import os
import sys
import time
import zipfile
from io import StringIO

from logger import *

class RecordData:
  def __init__(self, root_gz_file, name, contents):
    self.root_gz_file = root_gz_file
    self.name = name
    self.contents = contents


def find_and_write_matches_subprocess(record_queue, definitions, txt_locks, zip_files_with_matches):
    print(f"Starting search process #{os.getpid()}")

    if zip_files_with_matches:
        zip_archives = {}
        findings_dir = os.path.dirname(definitions[0][0])
        zip_process_dir = os.path.join(f"{findings_dir}\\temp", str(os.getpid()))
        os.makedirs(zip_process_dir)           

    txt_buffers = {}
    for txt_path, _ in definitions:
        txt_buffers[txt_path] = StringIO()
        if zip_files_with_matches:
            zip_path = os.path.join(zip_process_dir, f"{os.path.basename(os.path.splitext(txt_path)[0])}.zip")
            zip_archives[zip_path] = zipfile.ZipFile(zip_path, 'a', zipfile.ZIP_DEFLATED)

    while True:
        record_obj: RecordData = record_queue.get()
        if record_obj == None:
            for txt_path, _ in definitions:
                buffer_contents = txt_buffers[txt_path].getvalue()
                with txt_locks[txt_path]:
                    with open(txt_path, "a", encoding='utf-8') as output_file:
                        output_file.write(buffer_contents)
            if zip_files_with_matches:
                for zip_file in zip_archives:
                    zip_archives[zip_file].close()
                
            print(f"Ending search process #{os.getpid()}")
            break

        for txt_path, regex in definitions:
            matches_name = []
            matches_contents = []

            matches_name = find_regex_matches(record_obj.name, regex)

            if is_file_binary(record_obj.contents):
                # If the file is binary data (image, video, audio, etc), only search the file name, since searching the binary data is wasted effort
                matches_contents = ''
            else:    
                matches_contents = find_regex_matches(record_obj.contents.decode('utf-8', 'ignore'), regex)

            if matches_name or matches_contents:
                write_matches_to_txt_output_buffer(txt_buffers[txt_path], matches_name, matches_contents, record_obj.root_gz_file, record_obj.name)
                if zip_files_with_matches:
                    zip_path = os.path.join(zip_process_dir, f"{os.path.basename(os.path.splitext(txt_path)[0])}.zip")
                    write_file_with_match_to_zip(record_obj.contents, record_obj.name, zip_archives[zip_path])


def merge_zip_files(containing_dir, output_dir, definition_prefix):
    combined_zip = os.path.join(output_dir, f"{definition_prefix}.zip")
    added_files = set()

    for subdir, _, _ in os.walk(containing_dir):
        for file in glob.glob(os.path.join(subdir, f"{definition_prefix}*.zip")):
            with zipfile.ZipFile(file, 'r') as z1:
                with zipfile.ZipFile(combined_zip, 'a', compression=zipfile.ZIP_DEFLATED) as z2:
                    for file in z1.namelist():
                        if file not in added_files:
                            z2.writestr(file, z1.read(file))
                            added_files.add(file)


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


def write_file_with_match_to_zip(file_data, file_name, zip_archive):
    reformatted_file_name = reformat_file_name(file_name)
    if reformatted_file_name not in zip_archive.namelist():
        zip_archive.writestr(reformatted_file_name, file_data)


def is_file_binary(file_data):
    # Set of characters typically found in text files
    text_chars = bytearray({7, 8, 9, 10, 12, 13, 27} | set(range(0x20, 0x100)) - {0x7f})
    first_1024_chars = file_data[:1024]
    return bool(first_1024_chars.translate(None, text_chars))


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
        WarcSearcherLogger.log_info(f"Remaining items to search: {queue.qsize()}")
        time.sleep(5)