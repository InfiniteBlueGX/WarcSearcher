import glob
import os
import sys

from logger import *
from utilities import get_total_ram_bytes_rounded


def validate_and_get_config_ini_path() -> str:
    if os.path.isfile('config.ini'):
        config_path = 'config.ini'
    elif os.path.isfile('../config.ini'):
        config_path = '../config.ini'
    else:
        log_error("config.ini file does not exist in the working directory or its parent directory. Exiting.")
        sys.exit()

    return config_path


def validate_and_get_warc_gz_archives_directory(parsed_warc_gz_archives_directory: str) -> str:
    """Validates that the directory containing the warc.gz archives exists and has .gz files present."""
    if not os.path.exists(parsed_warc_gz_archives_directory):
        log_error(f"Directory containing the warc.gz archives to search does not exist: {parsed_warc_gz_archives_directory}. Exiting.")
        sys.exit()

    if not glob.glob(parsed_warc_gz_archives_directory + '/*.gz'):
        log_error(f"Directory that should contain the .gz archives to search does not contain any: {parsed_warc_gz_archives_directory}. Exiting.")
        sys.exit()

    return parsed_warc_gz_archives_directory


def validate_and_get_search_regex_definitions_directory(parsed_search_regex_definitions_directory: str) -> str:
    """Validates that the directory containing the regex definition text files exists and has .txt files present."""
    if not os.path.exists(parsed_search_regex_definitions_directory):
        log_error(f"Directory containing the regex definition .txt files to search with does not exist: {parsed_search_regex_definitions_directory}. Exiting.")
        sys.exit()

    if not glob.glob(parsed_search_regex_definitions_directory + '/*.txt'):
        log_error(f"Directory that should contain the regex definition .txt files to search with does not contain any: {parsed_search_regex_definitions_directory}. Exiting.")
        sys.exit()

    return parsed_search_regex_definitions_directory


def validate_and_get_results_output_directory(parsed_results_output_directory: str) -> str:
    """Validates that the directory to output the search results to exists."""
    if not os.path.exists(parsed_results_output_directory):
        log_error(f"Directory to output the search results to does not exist: {parsed_results_output_directory}. Exiting.")
        sys.exit()
    
    return parsed_results_output_directory


def validate_and_get_max_concurrent_search_processes(parsed_max_concurrent_search_processes: str) -> int:
    """
    Validates the maximum number of concurrent search processes config.ini value. 
    If invalid, sets it to the maximum logical processors available.
    """
    total_logical_processors = os.cpu_count()

    try:
        max_concurrent_search_processes = (
            total_logical_processors if parsed_max_concurrent_search_processes == "none" 
            else int(parsed_max_concurrent_search_processes)
        )

        if max_concurrent_search_processes <= 0 or max_concurrent_search_processes > total_logical_processors:
            raise ValueError()

    except ValueError:
        log_warning(
            f"Invalid value for MAX_CONCURRENT_SEARCH_PROCESSES in config.ini: {parsed_max_concurrent_search_processes}. "
            f"Setting number of concurrent search processes to maximum logical processors available on the PC."
        )
        max_concurrent_search_processes = total_logical_processors

    return max_concurrent_search_processes


def validate_and_get_max_ram_usage_bytes(parsed_max_ram_usage_bytes: str) -> int:
    """
    Validates the maximum RAM usage config.ini value for the program execution in bytes.
    If invalid, sets it to the total amount of RAM available on the machine.
    """
    total_machine_ram_in_bytes = get_total_ram_bytes_rounded()

    try:
        max_ram_usage_in_bytes = (
            total_machine_ram_in_bytes if parsed_max_ram_usage_bytes == "none" 
            else int(parsed_max_ram_usage_bytes)
        )

        if max_ram_usage_in_bytes <= 0 or max_ram_usage_in_bytes > total_machine_ram_in_bytes:
            raise ValueError()

    except ValueError:
        log_warning(
            f"Invalid value for MAX_RAM_USAGE_BYTES in config.ini: {parsed_max_ram_usage_bytes}. "
            f"Setting maximum RAM usage to the total amount of RAM available on the PC."
        )
        max_ram_usage_in_bytes = total_machine_ram_in_bytes

    return max_ram_usage_in_bytes