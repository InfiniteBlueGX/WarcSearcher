import configparser
import glob
import os
import sys

from logger import *

settings = {
    "WARC_GZ_ARCHIVES_DIRECTORY": '',
    "SEARCH_REGEX_DEFINITIONS_DIRECTORY": '',
    "RESULTS_OUTPUT_DIRECTORY": '',
    "ZIP_FILES_WITH_MATCHES": False,
    "MAX_CONCURRENT_SEARCH_PROCESSES": None,
    "MAX_RAM_USAGE_PERCENT": 90,
    "SEARCH_BINARY_FILES": False,
}


def read_config_ini_variables():
    """Reads the variables found in ther config.ini file after ensuring the config.ini file exists."""
    config_path = validate_and_get_config_ini_path()

    parser = configparser.ConfigParser()
    parser.read(config_path)

    try:
        read_required_config_ini_variables(parser)
        read_optional_config_ini_variables(parser)
        
    except Exception as e:
        log_error(f"Error reading the contents of the config.ini file: \n{e}")
        sys.exit()


def read_required_config_ini_variables(parser: configparser.ConfigParser):
    """Reads the required variables from the config.ini file, validates them, and sets them in the config dictionary."""
    parsed_warc_gz_archives_directory = parser.get('REQUIRED', 'WARC_GZ_ARCHIVES_DIRECTORY')
    settings["WARC_GZ_ARCHIVES_DIRECTORY"] = validate_and_get_warc_gz_archives_directory(parsed_warc_gz_archives_directory)

    parsed_search_regex_definitions_directory = parser.get('REQUIRED', 'SEARCH_REGEX_DEFINITIONS_DIRECTORY')
    settings["SEARCH_REGEX_DEFINITIONS_DIRECTORY"] = validate_and_get_search_regex_definitions_directory(parsed_search_regex_definitions_directory)
        
    parsed_results_output_directory = parser.get('REQUIRED', 'RESULTS_OUTPUT_DIRECTORY')
    settings["RESULTS_OUTPUT_DIRECTORY"] = validate_and_get_results_output_directory(parsed_results_output_directory)


def read_optional_config_ini_variables(parser: configparser.ConfigParser):
    """Reads the optional variables from the config.ini file and sets them in the config settings dictionary."""
    settings["ZIP_FILES_WITH_MATCHES"] = parser.getboolean('OPTIONAL', 'ZIP_FILES_WITH_MATCHES')

    parsed_max_concurrent_search_processes = parser.get('OPTIONAL', 'MAX_CONCURRENT_SEARCH_PROCESSES').lower()
    settings["MAX_CONCURRENT_SEARCH_PROCESSES"] = validate_and_get_max_concurrent_search_processes(parsed_max_concurrent_search_processes)

    parsed_max_ram_useage_bytes = parser.get('OPTIONAL', 'MAX_RAM_USAGE_PERCENT').lower()
    settings["MAX_RAM_USAGE_PERCENT"] = validate_and_get_max_ram_usage_percent(parsed_max_ram_useage_bytes)

    settings["SEARCH_BINARY_FILES"] = parser.getboolean('OPTIONAL', 'SEARCH_BINARY_FILES')


def validate_and_get_config_ini_path() -> str:
    """Validates and returns the path to the config.ini file."""
    if os.path.isfile('config.ini'):
        config_path = 'config.ini'
    elif os.path.isfile('../config.ini'):
        config_path = '../config.ini'
    else:
        log_error("config.ini file does not exist in the working directory or its parent directory. Exiting.")
        sys.exit()

    return config_path


def validate_and_get_warc_gz_archives_directory(parsed_warc_gz_archives_directory: str) -> str:
    """Validates and returns the config.ini value for the directory containing the warc.gz archives."""
    if not os.path.exists(parsed_warc_gz_archives_directory):
        log_error(f"Directory containing the warc.gz archives to search does not exist: {parsed_warc_gz_archives_directory}. Exiting.")
        sys.exit()

    if not glob.glob(parsed_warc_gz_archives_directory + '/*.gz'):
        log_error(f"Directory that should contain the .gz archives to search does not contain any: {parsed_warc_gz_archives_directory}. Exiting.")
        sys.exit()

    return parsed_warc_gz_archives_directory


def validate_and_get_search_regex_definitions_directory(parsed_search_regex_definitions_directory: str) -> str:
    """Validates and returns the config.ini value for the directory containing the regex definition text files."""
    if not os.path.exists(parsed_search_regex_definitions_directory):
        log_error(f"Directory containing the regex definition .txt files to search with does not exist: {parsed_search_regex_definitions_directory}. Exiting.")
        sys.exit()

    if not glob.glob(parsed_search_regex_definitions_directory + '/*.txt'):
        log_error(f"Directory that should contain the regex definition .txt files to search with does not contain any: {parsed_search_regex_definitions_directory}. Exiting.")
        sys.exit()

    return parsed_search_regex_definitions_directory


def validate_and_get_results_output_directory(parsed_results_output_directory: str) -> str:
    """Validates and returns the config.ini value for the directory to output the search results to."""
    if not os.path.exists(parsed_results_output_directory):
        log_error(f"Directory to output the search results to does not exist: {parsed_results_output_directory}. Exiting.")
        sys.exit()
    
    return parsed_results_output_directory


def validate_and_get_max_concurrent_search_processes(parsed_max_concurrent_search_processes: str) -> int:
    """
    Validates and returns the config.ini value for the maximum number of concurrent search processes. 
    If invalid, it defaults to the maximum logical processors available.
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
            f"Defaulting to the maximum number of logical processors available on the PC ({total_logical_processors})."
        )
        max_concurrent_search_processes = total_logical_processors

    return max_concurrent_search_processes


def validate_and_get_max_ram_usage_percent(parsed_max_ram_usage_percent: str) -> int:
    """
    Validates and returns the config.ini value for the maximum RAM usage percentage.
    If invalid, it defaults to 90% of the amount of RAM available on the machine.
    """
    try:
        max_ram_usage_percent = (
            100 if parsed_max_ram_usage_percent == "none" 
            else int(parsed_max_ram_usage_percent)
        )

        if max_ram_usage_percent <= 0 or max_ram_usage_percent > 100:
            raise ValueError()

    except ValueError:
        log_warning(
            f"Invalid value for MAX_RAM_USAGE_PERCENT in config.ini: {parsed_max_ram_usage_percent}. "
            f"Defaulting to 90% of the total RAM available on the machine."
        )
        max_ram_usage_percent = 90

    return max_ram_usage_percent