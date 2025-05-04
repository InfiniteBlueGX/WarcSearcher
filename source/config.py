import configparser

from validators import *

settings = {
    "WARC_GZ_ARCHIVES_DIRECTORY": '',
    "SEARCH_REGEX_DEFINITIONS_DIRECTORY": '',
    "RESULTS_OUTPUT_DIRECTORY": '',
    "ZIP_FILES_WITH_MATCHES": False,
    "MAX_CONCURRENT_SEARCH_PROCESSES": os.cpu_count(),
    "MAX_RAM_USAGE_BYTES": get_60_percent_ram_limit(),
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

    parsed_max_ram_useage_bytes = parser.get('OPTIONAL', 'MAX_RAM_USAGE_BYTES').lower()
    settings["MAX_RAM_USAGE_BYTES"] = validate_and_get_max_ram_usage_bytes(parsed_max_ram_useage_bytes)

    settings["SEARCH_BINARY_FILES"] = parser.getboolean('OPTIONAL', 'SEARCH_BINARY_FILES')