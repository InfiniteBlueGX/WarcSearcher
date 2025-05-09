import glob
import re

import config
from logger import *
from results import get_results_file_path


def get_definition_txt_files_list() -> list[str]:
    """Finds all search regex definition .txt files in the search definitions directory. Returns a list containing paths to each definition file"""
    return glob.glob(os.path.join(config.settings["SEARCH_REGEX_DEFINITIONS_DIRECTORY"], '*.txt'))


def compile_regex_pattern_from_definition_file(definition_file_path: str) -> tuple[re.Pattern | None, bool]:
    """Reads a regex pattern from a definition file and compiles it into a regex pattern object."""
    try:
        with open(definition_file_path, 'r', encoding='utf-8') as file:
            raw_regex = file.read().strip()
        
        try:
            regex_pattern = re.compile(raw_regex, re.IGNORECASE)
            return regex_pattern, True
        except re.error:
            log_error(f"Invalid regular expression found in {os.path.basename(definition_file_path)}. It will be ignored.")
            return None, False
            
    except IOError as e:
        log_error(f"Error reading file {os.path.basename(definition_file_path)}: {str(e)}")
        return None, False


def create_result_files_associated_with_regexes_dict() -> dict[str, re.Pattern]:
    """
    Creates a dictionary with entries based on the definition files. 
    Each key is a results text file path with a similar file name as the definition, and each value is a compiled regex pattern from the definition file.
    """
    definition_files = get_definition_txt_files_list()

    results_file_regex_pattern_dict = {}
    
    for definition_file_path in definition_files:
        regex_pattern, success = compile_regex_pattern_from_definition_file(definition_file_path)
        if success:
            results_filepath = get_results_file_path(definition_file_path)
            results_file_regex_pattern_dict[results_filepath] = regex_pattern
    
    if not results_file_regex_pattern_dict:
        log_error("No valid regex patterns were found in any of the definition files. Exiting.")
        sys.exit()
    
    return results_file_regex_pattern_dict