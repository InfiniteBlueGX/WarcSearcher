import time
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor,
                                as_completed, wait)
from io import StringIO
from multiprocessing import Manager

from config import *
from definitions import create_result_files_associated_with_regexes_dict
from fastwarc.stream_io import FileStream, GZipStream
from fastwarc.warc import ArchiveIterator
from warc_record import WarcRecord
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
                SEARCH_QUEUE.put(WarcRecord(parent_warc_gz_file=warc_gz_file_path, name=record_name, contents=record_content))

                if records_read % 1000 == 0:
                    #print(f"{SEARCH_QUEUE.qsize()} records in search queue")
                    log_info(f"Read {records_read} response records from the WARC in {os.path.basename(os.path.splitext(warc_gz_file_path)[0])}")
                    process = psutil.Process()
                    while get_total_memory_in_use(process) > config.settings["MAX_RAM_USAGE_BYTES"]:
                        log_warning(f"RAM usage is beyond maximum specified in config.ini. Will attempt to continue after 10 seconds to allow time for the search queue to clear...")
                        time.sleep(10)
    except Exception as e:
        log_error(f"Error ocurred when reading {warc_gz_file_path}: \n{e}")



def search_worker_process(search_queue, results_and_regexes_dict: dict, 
                         results_files_locks_dict: dict, zip_files_with_matches: bool):
    """
    Worker process that continuously searches for regex matches in records from the search queue.
    """
    print(f"Starting search process #{os.getpid()}")
    result_files_write_buffers, zip_archives_dict = initialize_process_resources(
        results_and_regexes_dict, 
        zip_files_with_matches
    )
    
    # Primary loop to await and process records from the search queue
    while True:
        # Get a record from the search queue. This will block execution until a record is available.
        record_data: WarcRecord = search_queue.get()
        
        if record_data is None:
            # If the record obtained from the search queue is None, the main process has signaled to stop searching.
            finalize_process_resources(
                results_and_regexes_dict, 
                results_files_locks_dict, 
                result_files_write_buffers, 
                zip_archives_dict
            )
            print(f"Ending search process #{os.getpid()}")
            break
        
        search_warc_record(
            record_data, 
            results_and_regexes_dict, 
            result_files_write_buffers, 
            zip_archives_dict, 
            zip_files_with_matches
        )



def initialize_process_resources(results_and_regexes_dict, zip_files_with_matches):
    """Initialize resources needed for the search process."""

    result_files_write_buffers = {
        results_file_path: StringIO() 
        for results_file_path in results_and_regexes_dict.keys()
    }
    
    zip_archives_dict = {}
    if zip_files_with_matches:
        results_dir = os.path.dirname(next(iter(results_and_regexes_dict.keys())))
        zip_process_dir = os.path.join(f"{results_dir}/temp", str(os.getpid()))
        os.makedirs(zip_process_dir)
        
        for results_file_path in results_and_regexes_dict.keys():
            zip_archive_path = os.path.join(
                zip_process_dir, 
                f"{os.path.basename(os.path.splitext(results_file_path)[0])}.zip"
            )
            zip_archives_dict[zip_archive_path] = zipfile.ZipFile(
                zip_archive_path, 'a', zipfile.ZIP_DEFLATED
            )
    
    return result_files_write_buffers, zip_archives_dict



def search_warc_record(record_data, results_and_regexes_dict, result_files_write_buffers, 
                  zip_archives_dict, zip_files_with_matches):
    """Processes a single record, searching for regex matches. If matches are found, they are written to the corresponding result file."""
    for results_file_path, regex in results_and_regexes_dict.items():
        # Find matches in the filename
        matches_name = find_regex_matches(record_data.name, regex)
        
        # Find matches in the contents
        matches_contents = []
        if not config.settings["SEARCH_BINARY_FILES"] and is_file_binary(record_data.contents):
            # Skip binary files if configured to do so
            matches_contents = ''
        else:
            matches_contents = find_regex_matches(
                record_data.contents.decode('utf-8', 'ignore'), 
                regex
            )
        
        # Handle matches if found
        if matches_name or matches_contents:
            write_matched_file_to_output_buffer(
                result_files_write_buffers[results_file_path], 
                matches_name, 
                matches_contents, 
                record_data.parent_warc_gz_file, 
                record_data.name
            )
            
            if zip_files_with_matches:
                zip_process_dir = os.path.dirname(next(iter(zip_archives_dict.keys())))
                zip_archive_path = os.path.join(
                    zip_process_dir, 
                    f"{os.path.basename(os.path.splitext(results_file_path)[0])}.zip"
                )
                add_file_to_zip_archive(
                    record_data.name, 
                    record_data.contents, 
                    zip_archives_dict[zip_archive_path]
                )



def finalize_process_resources(results_and_regexes_dict, results_files_locks_dict, 
                    result_files_write_buffers, zip_archives_dict):
    """Finalize the search worker process by writing output buffers to result files and closing zip archives."""
    for results_file_path in results_and_regexes_dict.keys():
        buffer_contents = result_files_write_buffers[results_file_path].getvalue()
        with results_files_locks_dict[results_file_path]:
            with open(results_file_path, "a", encoding='utf-8') as output_file:
                output_file.write(buffer_contents)
    
    for zip_file in zip_archives_dict:
        zip_archives_dict[zip_file].close()
