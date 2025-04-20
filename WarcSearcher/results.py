import datetime
import shutil

from zipped_results import create_temp_directory_for_zip_archives
import config
from logger import *

results_output_subdirectory = ''


def create_results_output_subdirectory():
    """Creates a timestamped subdirectory in the results output directory to store the search results for the current execution."""

    results_subdirectory_name = "WarcSearcher_Results_" + datetime.datetime.now().strftime('%m-%d-%y_%H_%M_%S')
    
    results_output_subdirectory_temp = os.path.join(config.settings["RESULTS_OUTPUT_DIRECTORY"], results_subdirectory_name)
    os.makedirs(results_output_subdirectory_temp)

    log_info(f"Results output folder created: {results_output_subdirectory_temp}")

    global results_output_subdirectory
    results_output_subdirectory = results_output_subdirectory_temp

    if config.settings["ZIP_FILES_WITH_MATCHES"]:
        create_temp_directory_for_zip_archives(results_output_subdirectory)


def get_results_txt_file_path(definition_file_path) -> str:
    """Returns a file path for the results text file based on the definition file name."""

    filename_without_extension = os.path.splitext(os.path.basename(definition_file_path))[0]
    output_filename = f"{filename_without_extension}_results.txt"
    return os.path.join(results_output_subdirectory, output_filename)


def initialize_result_txt_files(definitions_list):
    """Initialize the results text files by writing headers."""

    initialized_files = []
    for txt_path, regex in definitions_list:
        with open(txt_path, "a", encoding='utf-8') as output_file:
            write_result_file_header(output_file, txt_path, regex)
        initialized_files.append(txt_path)
    return initialized_files


def get_result_txt_file_write_locks(manager, result_txt_file_paths):
    """Create write locks for the specified paths to the results text files."""

    txt_locks = manager.dict()
    for txt_path in result_txt_file_paths:
        txt_locks[txt_path] = manager.Lock()
    return txt_locks


def write_result_file_header(output_file, txt_file_path, regex):
    """Writes the header for the results .txt file."""

    timestamp = datetime.datetime.now().strftime('%Y.%m.%d %H:%M:%S')
    output_file.write(f'[{os.path.basename(txt_file_path)}]\n')
    output_file.write(f'[Created: {timestamp}]\n\n')
    output_file.write(f'[Regex used]\n{regex.pattern}\n\n')
    output_file.write('___________________________________________________________________\n\n')


def write_matched_file_to_result(output_buffer, matches_list_name, matches_list_contents, root_gz_file, containing_file):
    """Writes the matched file information to the output buffer."""

    output_buffer.write(f'[Archive: {root_gz_file}]\n')
    output_buffer.write(f'[File: {containing_file}]\n\n')

    write_matches_to_result(output_buffer, matches_list_name, 'file name')
    write_matches_to_result(output_buffer, matches_list_contents, 'file contents')

    output_buffer.write('___________________________________________________________________\n\n')


def write_matches_to_result(output_buffer, matches_list, match_type):
    """Writes the matches found to the output buffer."""

    if matches_list:
        unique_matches_set = [match for match in set(matches_list)]
        output_buffer.write(f'[Matches found in {match_type}: {len(matches_list)} ({len(matches_list)-len(unique_matches_set)} duplicates omitted)]\n')
        for i, match in enumerate(unique_matches_set, start=1):
            output_buffer.write(f'[Match #{i} in {match_type}]\n\n"{match}"\n\n')


def move_log_file_to_results_subdirectory():
    """Moves the log file to the results output subdirectory, or keeps it in the working directory if an output subdirectory was not created."""

    if results_output_subdirectory != '':
        working_directory_log_path = os.path.join(os.getcwd(), 'log.log')
        results_output_subdirectory_log_path = os.path.join(results_output_subdirectory, 'log.log')
        shutil.move(working_directory_log_path, results_output_subdirectory_log_path)
    else:
        # The results output subdirectory was not created likely due to an error, so keep the log file in the working directory.
        log_info(f"Log file output to: {os.getcwd()}\\log")


def log_results_output_path():
    if results_output_subdirectory != '':
        log_info(f"Results output to: {results_output_subdirectory}")