from asyncio import Future
from threading import Event, Thread
import time
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor,
                                as_completed, wait)
from io import StringIO
from multiprocessing import Manager
from typing import Any

from config import *
from fastwarc.stream_io import FileStream, GZipStream
from fastwarc.warc import ArchiveIterator, WarcRecordType
from warc_record import WarcRecord
from results import *
from utilities import *

SEARCH_QUEUE = None
TOTAL_RECORDS_READ: int = 0
PAUSE_READ_THREADS_EVENT = Event()


def perform_search():
    """
    Intiates the search by setting up resources and starting the search processes.
    Once the search processes complete, it finalizes the results zip archives if configured to do so.
    """
    warc_gz_files_list = glob.glob(f"{config.settings["WARC_GZ_ARCHIVES_DIRECTORY"]}/*.gz")

    results_and_regexes_dict = create_result_files_associated_with_regexes_dict()
    manager = Manager()

    write_result_files_headers(results_and_regexes_dict)
    result_files_write_locks_dict = create_result_files_write_locks_dict(manager, results_and_regexes_dict.keys())

    global SEARCH_QUEUE
    SEARCH_QUEUE = manager.Queue()

    initiate_search_worker_processes(warc_gz_files_list, results_and_regexes_dict, result_files_write_locks_dict)
    log_info("Finished Searching.")

    if config.settings["ZIP_FILES_WITH_MATCHES"]:
        finalize_results_zip_archives(results_and_regexes_dict.keys())


def initiate_search_worker_processes(gz_files_list: list, results_and_regexes_dict: dict, result_files_write_locks_dict: dict):
    """Initiates the search worker processes to search the WARC.gz records via multiprocessing."""
    max_worker_processes = calculate_max_search_worker_processes()
    log_info(f"Starting {max_worker_processes} worker processes to search the WARC.gz records, plus 1 to read them in.")

    with ProcessPoolExecutor(max_workers = max_worker_processes) as executor:
        futures = [executor.submit(search_worker_process, 
                                   SEARCH_QUEUE, 
                                   results_and_regexes_dict, 
                                   result_files_write_locks_dict,
                                   config.settings["ZIP_FILES_WITH_MATCHES"]) for _ in range(max_worker_processes)]

        # Main process execution: read the warc.gz files and put records into the search queue.
        initiate_warc_gz_read_threads(gz_files_list)
        
        print("\n")
        log_info("All records read from the WARC.gz files. Waiting on search worker processes to finish...")

        signal_worker_processes_to_stop(max_worker_processes) 
        print_remaining_search_queue_items()

        wait(futures)


def calculate_max_search_worker_processes() -> int:
    """Calculates the maximum number of worker processes to be used for searching the WARC.gz files."""
    return config.settings["MAX_CONCURRENT_SEARCH_PROCESSES"]-1 if config.settings["MAX_CONCURRENT_SEARCH_PROCESSES"] > 1 else 1


def initiate_warc_gz_read_threads(warc_gz_files: list):
    """Sets up threads to read up to 4 WARC.gz files simultaneously, as well as a thread to monitor the progress."""
    log_info(f"Reading records from {len(warc_gz_files)} WARC.gz files...\n")

    PAUSE_READ_THREADS_EVENT.set()
    with ThreadPoolExecutor(max_workers=4) as executor:
        tasks = {executor.submit(read_warc_gz_records, gz_file_path) for gz_file_path in warc_gz_files}

        monitor_thread = Thread(target=monitoring_thread, args=(tasks, config.settings["MAX_RAM_USAGE_BYTES"]))
        monitor_thread.start()

        for future in as_completed(tasks):
            future.result()

        monitor_thread.join()


def monitoring_thread(tasks: set[Future[None]], max_ram_usage_bytes: int):
    """
    Prints the total number of records and the current queue size at one second second intervals while the WARC.gz files are being read.
    Also performs a check at each interval to check the total RAM usage by the WarcSearcher process and its subprocesses.
    """
    while not all(future.done() for future in tasks):
        print(f"\rTotal WARC records read: {TOTAL_RECORDS_READ} | Records in the search queue: {SEARCH_QUEUE.qsize()}            ", end='', flush=True)
        monitor_processes_ram_usage(max_ram_usage_bytes)


def monitor_processes_ram_usage(max_ram_usage_bytes):
    """
    Monitors the RAM usage of the WarcSearcher process and its subprocesses. 
    If it exceeds the maximum specified in the config.ini, it pauses the read threads.
    """
    if get_total_ram_usage_by_process() > max_ram_usage_bytes:
        print("\n")
        log_warning(
                "RAM usage for the WarcSearcher process exceeds the maximum specified in the config.ini.\n"
                "Will wait 10 seconds to allow time for the records in the search queue to process.\n"
                "Consider increasing the MAX_CONCURRENT_SEARCH_PROCESSES or MAX_RAM_USAGE_BYTES values in the config.ini.\n"
            )
        PAUSE_READ_THREADS_EVENT.clear() # Pause the read threads when RAM usage exceeds the limit
        time.sleep(10)
    else:
        PAUSE_READ_THREADS_EVENT.set() # Resume the read threads when RAM usage is below the limit
        time.sleep(1)


def read_warc_gz_records(warc_gz_file_path: str):
    """Reads the records from the WARC.gz file and puts response records into the search queue."""
    # FastWARC optimization by using a FileStream + GZipStream like this: 
    # https://resiliparse.chatnoir.eu/en/stable/man/fastwarc.html#iterating-warc-files
    with FileStream(warc_gz_file_path, 'rb') as file_stream:
        with GZipStream(file_stream) as gz_file_stream:
            try:
                records = ArchiveIterator(
                    gz_file_stream, 
                    strict_mode=False, 
                    record_types=WarcRecordType.response
                )
                
                if not any(records):
                    log_warning(f"No WARC records found in {os.path.basename(warc_gz_file_path)}")
                    return

                for record in records:
                    PAUSE_READ_THREADS_EVENT.wait() # If the read threads are paused, wait until they are resumed

                    record_name = record.headers['WARC-Target-URI']
                    record_content = record.reader.read()
                    
                    global TOTAL_RECORDS_READ
                    TOTAL_RECORDS_READ += 1

                    SEARCH_QUEUE.put(
                        WarcRecord(
                            parent_warc_gz_file=warc_gz_file_path, 
                            name=record_name, 
                            contents=record_content
                        )
                    )

            except Exception as e:
                log_error(f"Error ocurred when reading {os.path.basename(warc_gz_file_path)}: \n{e}")


def search_worker_process(search_queue, results_and_regexes_dict: dict, 
                         results_files_locks_dict: dict, zip_files_with_matches: bool):
    """
    Worker process that awaits and retrieves records from the search queue. 
    It then searches the record name and contents against the regex definitions and writes any matches to the corresponding results output buffer.
    """
    result_files_write_buffers, zip_archives_dict = initialize_worker_process_resources(
        results_and_regexes_dict, 
        zip_files_with_matches
    )
    
    # Primary loop to await and process records from the search queue
    while True:
        # Get a record from the search queue. This will block execution until a record is available.
        warc_record: WarcRecord = search_queue.get()
        
        if warc_record is None:
            # If the record obtained from the search queue is None, the main process has signaled the worker processes to stop.
            finalize_worker_process_resources(
                results_and_regexes_dict, 
                results_files_locks_dict, 
                result_files_write_buffers, 
                zip_archives_dict
            )
            break
        
        search_warc_record(
            warc_record, 
            results_and_regexes_dict, 
            result_files_write_buffers, 
            zip_archives_dict, 
            zip_files_with_matches
        )


def initialize_worker_process_resources(results_and_regexes_dict: dict, zip_files_with_matches: bool):
    """Initialize resources used by a search worker process."""
    result_files_write_buffers = {
        results_file_path: StringIO() 
        for results_file_path in results_and_regexes_dict.keys()
    }
    
    zip_archives_dict = {}
    if zip_files_with_matches:
        results_dir = os.path.dirname(next(iter(results_and_regexes_dict.keys())))
        zip_temp_dir_for_process = os.path.join(f"{results_dir}/temp", str(os.getpid()))
        os.makedirs(zip_temp_dir_for_process)
        
        for results_file_path in results_and_regexes_dict.keys():
            zip_results_archive_path = os.path.join(
                zip_temp_dir_for_process, 
                f"{get_base_file_name(results_file_path)}.zip"
            )
            zip_archives_dict[zip_results_archive_path] = zipfile.ZipFile(
                zip_results_archive_path, 'a', zipfile.ZIP_DEFLATED
            )
    
    return result_files_write_buffers, zip_archives_dict



def search_warc_record(warc_record: WarcRecord, results_and_regexes_dict: dict, result_files_write_buffers: dict[Any, StringIO], 
                  zip_archives_dict: dict[str, zipfile.ZipFile], zip_files_with_matches: bool):
    """Processes a single WARC record, searching for regex matches. If matches are found, they are written to the corresponding result file."""
    for results_file_path, regex in results_and_regexes_dict.items():

        matches_in_name = find_regex_matches(warc_record.name, regex)
        matches_in_contents = []
        
        if not config.settings["SEARCH_BINARY_FILES"] and is_file_binary(warc_record.contents):
            # Skip binary files if configured to do so
            matches_in_contents = ''
        else:
            matches_in_contents = find_regex_matches(warc_record.contents.decode('utf-8', 'ignore'), regex)
        
        if matches_in_name or matches_in_contents:
            write_record_to_output_buffer(
                result_files_write_buffers[results_file_path], 
                matches_in_name, 
                matches_in_contents, 
                warc_record.parent_warc_gz_file, 
                warc_record.name
            )
            
            if zip_files_with_matches:
                zip_archive_path = get_results_zip_archive_file_path(zip_archives_dict, results_file_path)

                try:
                    add_file_to_zip_archive(
                        warc_record.name, 
                        warc_record.contents, 
                        zip_archives_dict[zip_archive_path]
                    )
                except Exception as e:
                    log_error(f"Error adding file to zip archive {zip_archive_path}: {e}")
                    continue


def finalize_worker_process_resources(results_and_regexes_dict: dict, results_files_locks_dict: dict, 
                    result_files_write_buffers: dict[Any, StringIO], zip_archives_dict: dict[str, zipfile.ZipFile]):
    """Finalize a search worker process' resources by writing output buffers to result files and closing zip archives."""
    for results_file_path in results_and_regexes_dict.keys():
        buffer_contents = result_files_write_buffers[results_file_path].getvalue()

        with results_files_locks_dict[results_file_path]:
            with open(results_file_path, "a", encoding='utf-8') as output_file:
                output_file.write(buffer_contents)
    
    for zip_file in zip_archives_dict:
        zip_archives_dict[zip_file].close()


def signal_worker_processes_to_stop(max_worker_processes: int):
    """Signals the worker processes to stop by putting None into the search queue."""
    for _ in range(max_worker_processes):
        SEARCH_QUEUE.put(None)


def print_remaining_search_queue_items():
    """Prints the remaining items in the search queue at one second intervals."""
    while SEARCH_QUEUE.qsize() > 0:
        # Extra spaces are needed to properly overwrite the previous line in the console
        print(f"\rRemaining records to search: {SEARCH_QUEUE.qsize()}            ", end='', flush=True)
        time.sleep(1)
        
    print(f"\rRemaining records to search: 0            \n\n", end='', flush=True)