import os
import zipfile

from zip_files import write_file_with_match_to_zip
from fileops import write_matches_to_txt_output_buffer
from utilities import *

from io import StringIO

class RecordData:
  def __init__(self, root_gz_file, name, contents):
    self.root_gz_file = root_gz_file
    self.name = name
    self.contents = contents


def find_and_write_matches_subprocess(record_queue, definitions, txt_locks, zip_files_with_matches):
    print(f"Starting search process #{os.getpid()}")

    if zip_files_with_matches:
        zip_archives = {}
        results_dir = os.path.dirname(definitions[0][0])
        zip_process_dir = os.path.join(f"{results_dir}\\temp", str(os.getpid()))
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