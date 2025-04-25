import time
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor,
                                as_completed, wait)
from io import StringIO
from multiprocessing import Manager

from config import *
from definitions import create_result_files_associated_with_regexes_dict
from fastwarc.stream_io import FileStream, GZipStream
from fastwarc.warc import ArchiveIterator
from record_data import RecordData
from results import *
from utilities import *

SEARCH_QUEUE = None

def start_search():
    results_and_regexes_dict = create_result_files_associated_with_regexes_dict()
    manager = Manager()

    write_results_file_headers(results_and_regexes_dict)
    result_files_write_locks_dict = get_result_files_write_locks_dict(manager, results_and_regexes_dict.keys())

    global SEARCH_QUEUE
    SEARCH_QUEUE = manager.Queue()

    max_processes: int = config.settings["MAX_SEARCH_PROCESSES"]-1 if config.settings["MAX_SEARCH_PROCESSES"] > 1 else 1

    with ProcessPoolExecutor(max_workers = max_processes) as executor:
        futures = [executor.submit(search_worker_process, 
                                   SEARCH_QUEUE, 
                                   results_and_regexes_dict, 
                                   result_files_write_locks_dict, 
                                   config.settings["ZIP_FILES_WITH_MATCHES"]) for _ in range(max_processes)]

        compile_and_read_warc_gz_files(config.settings["WARC_GZ_ARCHIVES_DIRECTORY"])

        # Put a None object in the queue for each process to signal them to stop searching
        for _ in range(max_processes):
            SEARCH_QUEUE.put(None)

        log_info("Waiting on search processes to finish - This may take a while, please wait...")

        wait(futures)

    if config.settings["ZIP_FILES_WITH_MATCHES"]:
        finalize_results_zip_archives(results_and_regexes_dict.keys())




def compile_and_read_warc_gz_files(gz_directory_path: str):
    gz_files = glob.glob(f"{gz_directory_path}/**/*.gz", recursive=True)

    validate_gz_files_exist(gz_directory_path, gz_files)

    # Set up 4 threads to read the gz files concurrently - one thread per gz file. 
    # Each thread will open a gz file and put records into the search queue.
    # TODO experiment with different values
    with ThreadPoolExecutor(max_workers = 4) as executor:
        tasks = {executor.submit(read_warc_gz_records, gz_file_path) for gz_file_path in gz_files}

        for future in as_completed(tasks):
            future.result()


def read_warc_gz_records(warc_gz_file_path: str):
    gz_file_stream = GZipStream(FileStream(warc_gz_file_path, 'rb'))
    log_info(f"Reading records from {warc_gz_file_path}")

    try:
        records = ArchiveIterator(gz_file_stream, strict_mode=False)
        if not any(records):
            log_warning(f"No WARC records found in {warc_gz_file_path}")
            return

        records_read = 0
        for record in records:
            if record.headers['WARC-Type'] == 'response':
                records_read += 1
                record_content = record.reader.read()
                record_name = record.headers['WARC-Target-URI']
                record_obj = RecordData(parent_warc_gz_file=warc_gz_file_path, name=record_name, contents=record_content)
                SEARCH_QUEUE.put(record_obj)

                if records_read % 1000 == 0:
                    log_info(f"Read {records_read} response records from the WARC in {warc_gz_file_path}")
                    process = psutil.Process()
                    while get_total_memory_in_use(process) > config.settings["MAX_RAM_USAGE_BYTES"]:
                        log_warning(f"RAM usage is beyond maximum specified in config.ini. Will attempt to continue after 10 seconds to allow time for the search queue to clear...")
                        time.sleep(10)
    except Exception as e:
        log_error(f"Error ocurred when reading {warc_gz_file_path}: \n{e}")


def search_worker_process(search_queue, results_and_regexes_dict: dict, results_files_locks_dict: dict, zip_files_with_matches: bool):
    """This function is intended to be run on any number of subprocesses to search for regex matches in the gz files."""
    print(f"Starting search process #{os.getpid()}")

    # Make a subdirectory in the temp zip directory for this processes' output of zipped results
    if zip_files_with_matches:
        # TODO move this out
        zip_archives_dict = {}
        results_dir = os.path.dirname(next(iter(results_and_regexes_dict.keys())))
        zip_process_dir = os.path.join(f"{results_dir}\\temp", str(os.getpid()))
        os.makedirs(zip_process_dir)           

    # Set up the output buffers for each regex result file and make an empty zip archive for each regex result file
    result_files_write_buffers = {}
    for results_file_path in results_and_regexes_dict.keys():
        result_files_write_buffers[results_file_path] = StringIO()
        if zip_files_with_matches:
            zip_archive_path = os.path.join(zip_process_dir, f"{os.path.basename(os.path.splitext(results_file_path)[0])}.zip")
            zip_archives_dict[zip_archive_path] = zipfile.ZipFile(zip_archive_path, 'a', zipfile.ZIP_DEFLATED)

    # Primary loop to await and process records from the search queue
    while True:
        # Get a record from the search queue. This will block execution until a record is available.
        record_data: RecordData = search_queue.get()

        if record_data == None:
            # If the record obtained from the search queue is None, the main process has signaled to stop searching.
            for results_file_path in results_and_regexes_dict.keys():
                buffer_contents = result_files_write_buffers[results_file_path].getvalue()
                with results_files_locks_dict[results_file_path]:
                    # Write the contents of the buffer to the results file once finished searching
                    with open(results_file_path, "a", encoding='utf-8') as output_file:
                        output_file.write(buffer_contents)
            if zip_files_with_matches:
                for zip_file in zip_archives_dict:
                    zip_archives_dict[zip_file].close()
                
            print(f"Ending search process #{os.getpid()}")
            break

        for results_file_path, regex in results_and_regexes_dict.items():
            matches_name = []
            matches_contents = []

            matches_name = find_regex_matches(record_data.name, regex)

            if is_file_binary(record_data.contents):
                # If the file is binary data (image, video, audio, etc), only search the file name
                # TODO maybe make this an optional config.ini setting
                matches_contents = ''
            else:    
                matches_contents = find_regex_matches(record_data.contents.decode('utf-8', 'ignore'), regex)

            if matches_name or matches_contents:
                #print(f"Writing record on process #{os.getpid()}")
                write_matched_file_to_result(result_files_write_buffers[results_file_path], matches_name, matches_contents, record_data.parent_warc_gz_file, record_data.name)
                if zip_files_with_matches:
                    zip_archive_path = os.path.join(zip_process_dir, f"{os.path.basename(os.path.splitext(results_file_path)[0])}.zip")
                    add_file_to_zip_archive(record_data.name, record_data.contents, zip_archives_dict[zip_archive_path])
