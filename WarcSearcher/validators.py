import glob
import os
import sys

from logger import *


def validate_warc_gz_archives_directory(warc_gz_archives_directory):
    """Validates that the directory containing the .gz archives exists and has .gz files present."""

    if not os.path.exists(warc_gz_archives_directory):
        WarcSearcherLogger.log_error(f"Directory containing the .gz archives to search does not exist: {warc_gz_archives_directory}")
        sys.exit()
    if not glob.glob(warc_gz_archives_directory + '/*.gz'):
        WarcSearcherLogger.log_error(f"Directory that should contain the .gz archives to search does not contain any: {warc_gz_archives_directory}")
        sys.exit()


def validate_search_definitions_directory(search_definitions_directory):
    """Validates that the directory containing the regex definition .txt files exists and has .txt files present."""

    if not os.path.exists(search_definitions_directory):
        WarcSearcherLogger.log_error(f"Directory containing the regex definition .txt files does not exist: {search_definitions_directory}")
        sys.exit()
    if not glob.glob(search_definitions_directory + '/*.txt'):
        WarcSearcherLogger.log_error(f"Directory that should contain the regex definition .txt files does not contain any: {search_definitions_directory}")
        sys.exit()


def validate_results_output_directory(results_output_directory):
    """Validates that the directory to output the search results to exists."""

    if not os.path.exists(results_output_directory):
        WarcSearcherLogger.log_error(f"Directory containing the search results does not exist: {results_output_directory}")
        sys.exit()


def validate_regex_patterns(regex_patterns_list: list):
    """
    Validate that at least one valid regex pattern exists.
    If no valid patterns are found, log an error and exit the program.
    """
    if not regex_patterns_list:
        WarcSearcherLogger.log_error("There are no valid regular expressions in any of the definition files - terminating execution.")
        sys.exit()