import glob
import re

import config
from logger import *
from results import get_results_file_path
from validators import verify_regex_patterns_exist


def get_definition_txt_files_list():
    """Finds all definition files in the search definitions directory. Returns a list of paths to definition files"""
    return glob.glob(os.path.join(config.settings["SEARCH_REGEX_DEFINITIONS_DIRECTORY"], '*.txt'))


def compile_regex_pattern_from_definition_file(definition_file):
    """Reads a regex pattern from a definition file and compiles it into a regex object."""
    try:
        with open(definition_file, 'r', encoding='utf-8') as file:
            raw_regex = file.read().strip()
        
        try:
            regex_pattern = re.compile(raw_regex, re.IGNORECASE)
            return regex_pattern, True
        except re.error:
            log_error(f"Invalid regular expression found in {definition_file}")
            return None, False
            
    except IOError as e:
        log_error(f"Error reading file {definition_file}: {str(e)}")
        return None, False


def create_result_files_associated_with_regexes_dict() -> dict[str, re.Pattern]:
    """
    Creates a dictionary that maps result text file paths to their corresponding compiled regex patterns.
    
    This function reads definition files, compiles regex patterns from them, and maps
    each successfully compiling regex pattern to its corresponding result text file path.
    
    Returns:
        A dictionary where each key is an output file path and each value is a compiled regex pattern.
    """
    definition_files = get_definition_txt_files_list()
    results_file_regex_pattern_dict = {}
    
    for definition_file_path in definition_files:
        pattern, success = compile_regex_pattern_from_definition_file(definition_file_path)
        if success:
            results_filepath = get_results_file_path(definition_file_path)
            results_file_regex_pattern_dict[results_filepath] = pattern
        else:
            log_warning(
                f"Regex pattern in {definition_file_path} was invalid and will not be used to search."
            )
    
    verify_regex_patterns_exist(results_file_regex_pattern_dict)
    
    return results_file_regex_pattern_dict