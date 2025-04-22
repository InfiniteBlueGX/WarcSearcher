import glob
import re

import config
from logger import *
from results import get_results_txt_file_path
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


def create_associated_result_files_with_regex_list() -> list[tuple[str, re.Pattern]]:
    """
    Creates a collection of tuples that pair result text file paths with their corresponding compiled regex patterns.
    
    This function reads definition files, compiles regex patterns from them, and pairs
    each successfuly compiling regex pattern with its corresponding result text file path.
    
    Returns:
        A list of tuples where each tuple contains (output_file_path, compiled_regex_pattern)
    """
    definition_files = get_definition_txt_files_list()
    result_file_regex_pattern_tuples_list = []
    
    for file in definition_files:
        pattern, success = compile_regex_pattern_from_definition_file(file)
        if success:
            result_filepath = get_results_txt_file_path(file)
            result_file_regex_pattern_tuples_list.append((result_filepath, pattern))
        else:
            log_warning(
                f"Regex pattern in {file} was invalid and will not be used to search."
            )
    
    verify_regex_patterns_exist(result_file_regex_pattern_tuples_list)
    
    return result_file_regex_pattern_tuples_list