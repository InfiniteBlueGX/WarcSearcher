import configparser

from validators import *

settings = {
    "WARC_GZ_ARCHIVES_DIRECTORY": '',
    "SEARCH_REGEX_DEFINITIONS_DIRECTORY": '',
    "RESULTS_OUTPUT_DIRECTORY": '',
    "ZIP_FILES_WITH_MATCHES": False,
    "MAX_SEARCH_PROCESSES": 2,
    "MAX_RAM_USAGE_BYTES": 1000000000,
    "SEARCH_BINARY_FILES": False,
}


def read_config_ini_variables():
    """Reads the variables found in ther config.ini file after ensuring it exists."""
    config_path = get_config_ini_path()

    parser = configparser.ConfigParser()
    parser.read(config_path)

    try:
        read_required_config_ini_variables(parser)
        read_optional_config_ini_variables(parser)
        
    except Exception as e:
        log_error(f"Error reading the contents of the config.ini file: \n{e}")
        sys.exit()


def get_config_ini_path():
    if os.path.isfile('config.ini'):
        config_path = 'config.ini'
    elif os.path.isfile('../config.ini'):
        config_path = '../config.ini'
    else:
        log_error("config.ini file does not exist in the working directory or its parent.")
        sys.exit()
    return config_path


def read_required_config_ini_variables(parser: configparser.ConfigParser):
    """Reads the required variables from the config.ini file, validates them, and sets them in the config dictionary."""
    settings["WARC_GZ_ARCHIVES_DIRECTORY"] = parser.get('REQUIRED', 'WARC_GZ_ARCHIVES_DIRECTORY')
    validate_warc_gz_archives_directory(settings["WARC_GZ_ARCHIVES_DIRECTORY"])

    settings["SEARCH_REGEX_DEFINITIONS_DIRECTORY"] = parser.get('REQUIRED', 'SEARCH_REGEX_DEFINITIONS_DIRECTORY')
    validate_search_regex_definitions_directory(settings["SEARCH_REGEX_DEFINITIONS_DIRECTORY"])
        
    settings["RESULTS_OUTPUT_DIRECTORY"] = parser.get('REQUIRED', 'RESULTS_OUTPUT_DIRECTORY')
    validate_results_output_directory(settings["RESULTS_OUTPUT_DIRECTORY"])


def read_optional_config_ini_variables(parser: configparser.ConfigParser):
    """Reads the optional variables from the config.ini file and sets them in the config settings dictionary."""
    settings["ZIP_FILES_WITH_MATCHES"] = parser.getboolean('OPTIONAL', 'ZIP_FILES_WITH_MATCHES')

    parsed_max_search_processes = parser.get('OPTIONAL', 'MAX_CONCURRENT_SEARCH_PROCESSES').lower()
    settings["MAX_SEARCH_PROCESSES"] = validate_and_get_max_search_processes(parsed_max_search_processes)

    parsed_max_ram_useage_bytes = parser.get('OPTIONAL', 'MAX_RAM_USAGE_BYTES').lower()
    settings["MAX_RAM_USAGE_BYTES"] = validate_and_get_max_ram_usage(parsed_max_ram_useage_bytes)

    settings["SEARCH_BINARY_FILES"] = parser.getboolean('OPTIONAL', 'SEARCH_BINARY_FILES')