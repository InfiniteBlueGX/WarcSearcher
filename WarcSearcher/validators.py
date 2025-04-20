import glob
import os
import sys

import logger
from logger import *
from utilities import get_total_ram_bytes_rounded


def validate_warc_gz_archives_directory(warc_gz_archives_directory):
    """Validates that the directory containing the .gz archives exists and has .gz files present."""

    if not os.path.exists(warc_gz_archives_directory):
        log_error(f"Directory containing the .gz archives to search does not exist: {warc_gz_archives_directory}")
        sys.exit()
    if not glob.glob(warc_gz_archives_directory + '/*.gz'):
        log_error(f"Directory that should contain the .gz archives to search does not contain any: {warc_gz_archives_directory}")
        sys.exit()


def validate_search_definitions_directory(search_definitions_directory):
    """Validates that the directory containing the regex definition .txt files exists and has .txt files present."""

    if not os.path.exists(search_definitions_directory):
        log_error(f"Directory containing the regex definition .txt files does not exist: {search_definitions_directory}")
        sys.exit()
    if not glob.glob(search_definitions_directory + '/*.txt'):
        log_error(f"Directory that should contain the regex definition .txt files does not contain any: {search_definitions_directory}")
        sys.exit()


def validate_results_output_directory(results_output_directory):
    """Validates that the directory to output the search results to exists."""

    if not os.path.exists(results_output_directory):
        log_error(f"Directory containing the search results does not exist: {results_output_directory}")
        sys.exit()


def validate_gz_file_existence(gz_directory_path, gz_files):
    if not gz_files:
        log_error(f"No .gz files were found at the root or any subdirectories of: {gz_directory_path}")
        sys.exit()


def verify_regex_patterns_exist(regex_patterns_list: list):
    """
    Validate that at least one valid regex pattern exists in the provided list.
    If no valid patterns are found, log an error and exit the program.
    """
    if not regex_patterns_list:
        log_error("There are no valid regular expressions in any of the definition files - terminating execution.")
        sys.exit()


def validate_and_get_max_search_processes(parsed_value):
    """Validates and returns the maximum number of search processes."""

    # Calculate total logical processors available on the system
    total_logical_processors = os.cpu_count()

    try:
        max_search_processes = (
            total_logical_processors if parsed_value == "none" 
            else int(parsed_value)
        )

        if max_search_processes <= 0 or max_search_processes > total_logical_processors:
            raise ValueError()

    except ValueError:
        log_warning(
            f"Invalid value for max_concurrent_search_processes in config.ini: {parsed_value}. "
            f"Setting number of search processes to maximum logical processors available on the PC."
        )
        max_search_processes = total_logical_processors

    return max_search_processes


def validate_and_get_target_ram_usage(parsed_value):
    """Validates and returns the target RAM usage for the program execution in bytes."""

    # Calculate total machine RAM in bytes, rounded down to nearest GB
    total_machine_ram_in_bytes = get_total_ram_bytes_rounded()

    try:
        target_process_ram_in_bytes = (
            total_machine_ram_in_bytes if parsed_value == "none" 
            else int(parsed_value)
        )

        if target_process_ram_in_bytes <= 0 or target_process_ram_in_bytes > total_machine_ram_in_bytes:
            raise ValueError()

    except ValueError:
        log_warning(
            f"Invalid value for target_ram_usage_bytes in config.ini: {parsed_value}. "
            f"Setting target RAM usage to maximum RAM available on the PC."
        )
        target_process_ram_in_bytes = total_machine_ram_in_bytes

    return target_process_ram_in_bytes