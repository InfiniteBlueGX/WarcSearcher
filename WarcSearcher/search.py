import time
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor,
                                as_completed, wait)
from io import StringIO
from multiprocessing import Manager

import results
from config import *
from fastwarc.stream_io import FileStream, GZipStream
from fastwarc.warc import ArchiveIterator
from record_data import RecordData
from results import *
from utilities import *
from zipped_results import *

SEARCH_QUEUE = None

def begin_search(definitions_list):
    manager = Manager()

    initialized_txt_files = initialize_result_txt_files(definitions_list)
    txt_locks = get_result_txt_file_write_locks(manager, initialized_txt_files)

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

        log_info("Waiting on search processes to finish - This may take a while, please wait...")

        wait(futures)

    if config.settings["ZIP_FILES_WITH_MATCHES"]:
        log_info("Finalizing the zip archives...")
        tempdir = os.path.join(results.results_output_subdirectory, "temp")
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(merge_zip_archives, 
                                       tempdir,
                                       results.results_output_subdirectory, 
                                       os.path.basename(os.path.splitext(txt_path)[0])): txt_path for txt_path, _ in definitions_list}
            for future in as_completed(futures):
                future.result()

        shutil.rmtree(tempdir)


def find_and_write_matches_subprocess(record_queue, definitions, txt_locks, zip_files_with_matches):
    """This function is intended to be run on any number of subprocesses to search for regex matches in the gz files."""
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
                write_matched_file_to_result(txt_buffers[txt_path], matches_name, matches_contents, record_obj.root_gz_file, record_obj.name)
                if zip_files_with_matches:
                    zip_path = os.path.join(zip_process_dir, f"{os.path.basename(os.path.splitext(txt_path)[0])}.zip")
                    write_file_with_match_to_zip(record_obj.contents, record_obj.name, zip_archives[zip_path])


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
    log_info(f"Beginning to process {gz_file_path}")

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
                record_obj = RecordData(root_gz_file=gz_file_path, name=record_name, contents=record_content)
                SEARCH_QUEUE.put(record_obj)

                if records_searched % 1000 == 0:
                    log_info(f"Read {records_searched} response records from the WARC in {gz_file_path}")
                    process = psutil.Process()
                    while get_total_memory_in_use(process) > config.settings["TARGET_RAM_USAGE_BYTES"]:
                        log_warning(f"RAM usage is beyond target size specified in config.ini. Will attempt to continue after 10 seconds to allow time to process the existing queue...")
                        time.sleep(10)
    except Exception as e:
        log_error(f"Error ocurred when reading contents of {gz_file_path}: \n{e}")