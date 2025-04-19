
import datetime
import glob
import os
import shutil

import config
import logger

from validators import *
from config import *


results_output_subdirectory = ''

def create_results_output_subdirectory():
    """Creates a timestamped subdirectory in the results output directory to store the search results for the current execution."""

    results_subdirectory_name = "WarcSearcher_Results_" + datetime.datetime.now().strftime('%m-%d-%y_%H_%M_%S')
    
    results_output_subdirectory = os.path.join(config.settings["RESULTS_OUTPUT_DIRECTORY"], results_subdirectory_name)
    os.makedirs(results_output_subdirectory)

    logger.log_info(f"Results output folder created: {results_output_subdirectory}")
    return results_output_subdirectory



def get_results_txt_file_path(definition_file) -> str:
    """
    Returns a file path for the results .txt file based on the definition file name.
    """
    filename_without_extension = os.path.splitext(os.path.basename(definition_file))[0]
    output_filename = f"{filename_without_extension}_results.txt"
    return os.path.join(results_output_subdirectory, output_filename)



def move_log_file_to_results_subdirectory():
    """Moves the log file to the results output subdirectory, or keeps it in the working directory if an output subdirectory was not created."""

    if results_output_subdirectory != '':
        working_directory_log_path = os.path.join(os.getcwd(), 'output_log.log')
        results_output_subdirectory_log_path = os.path.join(results_output_subdirectory, 'output_log.log')
        shutil.move(working_directory_log_path, results_output_subdirectory_log_path)
    else:
        # Keep log file in the working directory if no results subdirectory was created as part of the execution
        logger.log_info(f"Log file output to working directory: {os.getcwd()}\\output_log")



def get_definition_txt_files_list():
    """
    Find all definition files in the search definitions directory. Returns a list of paths to definition files
    """
    return glob.glob(os.path.join(config.settings["SEARCH_DEFINITIONS_DIRECTORY"], '*.txt'))



def initialize_result_txt_files_and_locks(manager, definitions_list):
    txt_locks = manager.dict()
    
    for txt_path, regex in definitions_list:
        with open(txt_path, "a", encoding='utf-8') as output_file:
            initialize_result_txt_file(output_file, txt_path, regex)
        txt_locks[txt_path] = manager.Lock()

    return txt_locks



def initialize_result_txt_file(output_file, txt_file_path, regex):
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