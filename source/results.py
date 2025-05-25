from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime
import glob
from io import StringIO
from multiprocessing.managers import SyncManager
import re
import shutil
from typing import Iterable

from utilities import get_base_file_name, merge_zip_archives
import config
from logger import *

results_output_subdirectory = ''


def create_result_files_associated_with_regexes_dict() -> dict[str, re.Pattern]:
    """
    Creates a dictionary with entries based on the definition files. 
    Each key is a results text file path with a similar file name as the definition, 
    and each value is a compiled regex pattern from the definition file.
    """
    definition_files = get_definition_txt_files_list()

    results_file_regex_pattern_dict = {}
    
    for definition_file_path in definition_files:
        regex_pattern, success = compile_regex_pattern_from_definition_file(definition_file_path)
        if success:
            results_filepath = get_results_file_path(definition_file_path)
            results_file_regex_pattern_dict[results_filepath] = regex_pattern
    
    if not results_file_regex_pattern_dict:
        log_error("No valid regex patterns were found in any of the definition files. Exiting.")
        sys.exit()
    
    return results_file_regex_pattern_dict


def get_definition_txt_files_list() -> list[str]:
    """Finds all search regex definition .txt files in the search definitions directory. Returns a list containing paths to each definition file"""
    return glob.glob(os.path.join(config.settings["SEARCH_REGEX_DEFINITIONS_DIRECTORY"], '*.txt'))


def compile_regex_pattern_from_definition_file(definition_file_path: str) -> tuple[re.Pattern | None, bool]:
    """Reads a regex pattern from a definition file and compiles it into a regex pattern object."""
    try:
        with open(definition_file_path, 'r', encoding='utf-8') as file:
            raw_regex = file.read().strip()
        
        try:
            regex_pattern = re.compile(raw_regex, re.IGNORECASE)
            return regex_pattern, True
        except re.error:
            log_error(f"Invalid regular expression found in {os.path.basename(definition_file_path)}. It will be ignored.")
            return None, False
            
    except IOError as e:
        log_error(f"Error reading file {os.path.basename(definition_file_path)}: {str(e)}")
        return None, False


def initialize_results_output_subdirectory():
    """
    Creates and initializes a timestamped subdirectory in the results output directory 
    where the search results for the current execution will be output to.
    """
    results_subdirectory_name = "WarcSearcher_Results_" + datetime.datetime.now().strftime('%m-%d-%y_%H_%M_%S')
    
    results_output_subdirectory_path = os.path.join(config.settings["RESULTS_OUTPUT_DIRECTORY"], results_subdirectory_name)
    os.makedirs(results_output_subdirectory_path)

    log_info(f"Results output folder created: {results_output_subdirectory_path}")

    global results_output_subdirectory
    results_output_subdirectory = results_output_subdirectory_path

    if config.settings["ZIP_FILES_WITH_MATCHES"]:
        os.makedirs(os.path.join(results_output_subdirectory, "temp"))


def get_results_file_path(definition_file_path: str) -> str:
    """Returns a file path for a results text file with a name similar to that of the corresponding definition file's name."""
    results_file_name = f"{get_base_file_name(definition_file_path)}_results.txt"
    return os.path.join(results_output_subdirectory, results_file_name)


def create_result_files_write_locks_dict(manager: SyncManager, results_file_paths: Iterable[str]) -> dict:
    """Create write locks for the specified paths to the results files."""
    write_locks_dict = manager.dict()
    for txt_path in results_file_paths:
        write_locks_dict[txt_path] = manager.Lock()
    return write_locks_dict


def write_result_files_headers(results_and_regexes_dict: dict[str, re.Pattern]):
    """Initialize the results text files by writing headers."""
    for results_file_path, regex in results_and_regexes_dict.items():
        with open(results_file_path, "a", encoding='utf-8') as results_file:
            timestamp = datetime.datetime.now().strftime('%Y.%m.%d %H:%M:%S')
            results_file.write(f'[{os.path.basename(results_file_path)}]\n')
            results_file.write(f'[Created: {timestamp}]\n\n')
            results_file.write(f'[Regex used]\n{regex.pattern}\n\n')
            results_file.write('___________________________________________________________________\n\n')


def write_record_info_to_result_output_buffer(output_buffer: StringIO, matches_list_name: list, matches_list_contents: list, parent_warc_gz_file: str, file_name: str):
    """Writes the matched record information to the output buffer."""
    output_buffer.write(f'[Archive: {parent_warc_gz_file}]\n')
    output_buffer.write(f'[File: {file_name}]\n\n')

    write_matches_to_result_output_buffer(output_buffer, matches_list_name, 'file name')
    write_matches_to_result_output_buffer(output_buffer, matches_list_contents, 'file contents')

    output_buffer.write('___________________________________________________________________\n\n')


def write_matches_to_result_output_buffer(output_buffer: StringIO, matches_list: list, match_type: str):
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
    """
    Logs the path to the results subdirectory if one exists. If the subdirectory doesn't exist,
    log a notification that one was not created and the path to the log file in the current working directory.
    """
    if results_output_subdirectory != '':
        log_info(f"Results output to: {results_output_subdirectory}")
    else:
        log_info(f"No results folder was created due to an error. Log file output to: {os.getcwd()}")


def get_results_zip_archive_file_path(zip_archives_dict: dict, results_file_path: str) -> str:
    """Returns a zip archive file path based on the name of the provided results file path."""
    temp_process_subdir_for_zip = os.path.dirname(next(iter(zip_archives_dict.keys())))
    zip_archive_path = os.path.join(
                    temp_process_subdir_for_zip, 
                    f"{get_base_file_name(results_file_path)}.zip"
                )
    
    return zip_archive_path


def finalize_results_zip_archives(results_file_paths: Iterable[str]):
    """Delegates multiple threads to merge all identically named zip archives output from the search worker processes."""
    log_info("Finalizing the zip archives, please wait...")
    tempdir = os.path.join(results_output_subdirectory, "temp")
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(merge_zip_archives, 
                                       tempdir,
                                       results_output_subdirectory, 
                                       get_base_file_name(results_path)): results_path for results_path in results_file_paths}
        for future in as_completed(futures):
            future.result()

    shutil.rmtree(tempdir)