import configparser
import logger
from validators import *


settings = {
    "WARC_GZ_ARCHIVES_DIRECTORY": '',
    "SEARCH_QUERIES_DIRECTORY": '',
    "RESULTS_OUTPUT_DIRECTORY": '',
    "ZIP_FILES_WITH_MATCHES": False,
    "MAX_ARCHIVE_READ_THREADS": 2,
    "MAX_SEARCH_PROCESSES": 2,
    "TARGET_PROCESS_MEMORY": 1000000000
}


def read_config_ini_variables():
    """Reads the variables found in ther config.ini file after ensuring it exists."""

    config_path = None
    if os.path.isfile('config.ini'):
        config_path = 'config.ini'
    elif os.path.isfile('../config.ini'):
        config_path = '../config.ini'
    else:
        logger.log_error("config.ini file does not exist in the working directory or its parent.")
        sys.exit()

    parser = configparser.ConfigParser()
    parser.read(config_path)

    try:
        read_required_config_ini_variables(parser)
        read_optional_config_ini_variables(parser)
        
    except Exception as e:
        logger.log_error(f"Error reading the contents of the config.ini file: \n{e}")
        sys.exit()



def read_required_config_ini_variables(parser):
    """Reads the required variables from the config.ini file, validates them, and sets them in the config dictionary."""

    settings["WARC_GZ_ARCHIVES_DIRECTORY"] = parser.get('REQUIRED', 'warc_gz_archives_directory')
    validate_warc_gz_archives_directory(settings["WARC_GZ_ARCHIVES_DIRECTORY"])

    settings["SEARCH_DEFINITIONS_DIRECTORY"] = parser.get('REQUIRED', 'search_definitions_directory')
    validate_search_definitions_directory(settings["SEARCH_DEFINITIONS_DIRECTORY"])
        
    settings["RESULTS_OUTPUT_DIRECTORY"] = parser.get('REQUIRED', 'results_output_directory')
    validate_results_output_directory(settings["RESULTS_OUTPUT_DIRECTORY"])



def read_optional_config_ini_variables(parser):
    """Reads the optional variables from the config.ini file and sets them in the config dictionary."""

    settings["ZIP_FILES_WITH_MATCHES"] = parser.getboolean('OPTIONAL', 'zip_files_with_matches')

    # TODO maybe remove this and just use the default value of 4 for max_archive_read_threads.
    threads_item = parser.get('OPTIONAL', 'max_concurrent_archive_read_threads').lower()
    settings["MAX_ARCHIVE_READ_THREADS"] = min(32, os.cpu_count() + 4) if threads_item == "none" else int(threads_item)

    processes_item = parser.get('OPTIONAL', 'max_concurrent_search_processes').lower()
    settings["MAX_SEARCH_PROCESSES"] = os.cpu_count() if processes_item == "none" else int(processes_item)

    process_memory_item = parser.get('OPTIONAL', 'target_process_memory_bytes').lower()
    settings["TARGET_PROCESS_MEMORY"] = 32000000000 if process_memory_item == "none" else int(process_memory_item)