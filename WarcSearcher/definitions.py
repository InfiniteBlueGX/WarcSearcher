import glob
import os
import re

from validators import verify_regex_patterns_exist
from results import get_results_txt_file_path
import logger
import config


def get_definition_txt_files_list():
    """Finds all definition files in the search definitions directory. Returns a list of paths to definition files"""
    
    return glob.glob(os.path.join(config.settings["SEARCH_DEFINITIONS_DIRECTORY"], '*.txt'))



def compile_regex_pattern_from_definition_file(definition_file):
    """Reads a regex pattern from a definition file and compiles it into a regex object."""
    
    try:
        with open(definition_file, 'r', encoding='utf-8') as file:
            raw_regex = file.read().strip()
        
        try:
            regex_pattern = re.compile(raw_regex, re.IGNORECASE)
            return regex_pattern, True
        except re.error:
            logger.log_error(f"Invalid regular expression found in {definition_file}")
            return None, False
            
    except IOError as e:
        logger.log_error(f"Error reading file {definition_file}: {str(e)}")
        return None, False
    


def create_associated_definition_files_regex_list() -> list: 
    """Creates a list of tuples that associates the regex patterns with their respective results output .txt file paths."""

    regex_patterns_list = []
    results_txt_files_dict = {}

    definition_files = get_definition_txt_files_list()
    
    for definition_file_path in definition_files:
        regex_pattern, success = compile_regex_pattern_from_definition_file(definition_file_path)
        
        if success:
            regex_patterns_list.append(regex_pattern)
            
            output_filepath = get_results_txt_file_path(definition_file_path)
            results_txt_files_dict[output_filepath] = output_filepath
        else:
            logger.log_warning(f"Regex pattern in {definition_file_path} was invalid and will not be used to search.")

    verify_regex_patterns_exist(regex_patterns_list)

    return list(zip(results_txt_files_dict, regex_patterns_list))