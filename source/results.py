from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime
from multiprocessing.managers import SyncManager
import shutil

from utilities import get_file_base_name, merge_zip_archives
import config
from logger import *

results_output_subdirectory = ''


def initialize_results_output_subdirectory():
    """Creates a timestamped subdirectory in the results output directory to store the search results for the current execution."""
    results_subdirectory_name = "WarcSearcher_Results_" + datetime.datetime.now().strftime('%m-%d-%y_%H_%M_%S')
    
    results_output_subdirectory_path = os.path.join(config.settings["RESULTS_OUTPUT_DIRECTORY"], results_subdirectory_name)
    os.makedirs(results_output_subdirectory_path)

    log_info(f"Results output folder created: {results_output_subdirectory_path}")

    global results_output_subdirectory
    results_output_subdirectory = results_output_subdirectory_path

    if config.settings["ZIP_FILES_WITH_MATCHES"]:
        os.makedirs(os.path.join(results_output_subdirectory, "temp"))
        log_info("Temporary folder for zipped results created in the results subdirectory.")


def get_results_file_path(definition_file_path: str) -> str:
    """Returns a file path for the results text file with a name similar to that of the definition file's name."""
    results_file_name = f"{get_file_base_name(definition_file_path)}_results.txt"
    return os.path.join(results_output_subdirectory, results_file_name)


def get_result_files_write_locks_dict(manager: SyncManager, results_file_paths) -> dict:
    """Create write locks for the specified paths to the results files."""
    write_locks_dict = manager.dict()
    for txt_path in results_file_paths:
        write_locks_dict[txt_path] = manager.Lock()
    return write_locks_dict


def write_results_file_headers(definitions_dict: dict):
    """Initialize the results text files by writing headers."""
    for results_file_path, regex in definitions_dict.items():
        with open(results_file_path, "a", encoding='utf-8') as results_file:
            timestamp = datetime.datetime.now().strftime('%Y.%m.%d %H:%M:%S')
            results_file.write(f'[{os.path.basename(results_file_path)}]\n')
            results_file.write(f'[Created: {timestamp}]\n\n')
            results_file.write(f'[Regex used]\n{regex.pattern}\n\n')
            results_file.write('___________________________________________________________________\n\n')


def write_matched_file_to_output_buffer(output_buffer, matches_list_name, matches_list_contents, parent_warc_gz_file, containing_file):
    """Writes the matched file information to the output buffer."""

    output_buffer.write(f'[Archive: {parent_warc_gz_file}]\n')
    output_buffer.write(f'[File: {containing_file}]\n\n')

    write_matches_to_output_buffer(output_buffer, matches_list_name, 'file name')
    write_matches_to_output_buffer(output_buffer, matches_list_contents, 'file contents')

    output_buffer.write('___________________________________________________________________\n\n')


def write_matches_to_output_buffer(output_buffer, matches_list, match_type):
    """Writes the matches found to the output buffer."""
    if matches_list:
        unique_matches_set = [match for match in set(matches_list)]
        output_buffer.write(f'[Matches found in {match_type}: {len(matches_list)} ({len(matches_list)-len(unique_matches_set)} duplicates omitted)]\n')
        for i, match in enumerate(unique_matches_set, start=1):
            output_buffer.write(f'[Match #{i} in {match_type}]\n\n"{match}"\n\n')


def move_log_file_to_results_subdirectory():
    """Moves the log file to the results output subdirectory, or keeps it in the working directory if an output subdirectory was not created."""
    if os.path.exists(results_output_subdirectory):
        working_directory_log_path = os.path.join(os.getcwd(), 'log.log')
        results_output_subdirectory_log_path = os.path.join(results_output_subdirectory, 'log.log')
        shutil.move(working_directory_log_path, results_output_subdirectory_log_path)


def log_results_output_path():
    if results_output_subdirectory != '':
        log_info(f"Results output to: {results_output_subdirectory}")
    else:
        # The results output subdirectory was not created likely due to an error. 
        # Log the path to the log file in the current working directory instead.
        log_info(f"No results folder was created. Log file output to: {os.getcwd()}\\log")


def finalize_results_zip_archives(results_file_paths):
    log_info("Finalizing the zip archives, please wait...")
    tempdir = os.path.join(results_output_subdirectory, "temp")
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(merge_zip_archives, 
                                       tempdir,
                                       results_output_subdirectory, 
                                       get_file_base_name(results_path)): results_path for results_path in results_file_paths}
        for future in as_completed(futures):
            future.result()

    shutil.rmtree(tempdir)