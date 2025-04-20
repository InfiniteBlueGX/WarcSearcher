import atexit

from config import read_config_ini_variables
from definitions import create_associated_definition_files_regex_list
from results import *
from search import begin_search
from search_timer import SearchTimer

searchTimer = SearchTimer()


def finish():
    """
    Function to be called on program exit. Responsible for logging errors and warnings, closing the logging file handler, 
    and moving the log file to the results subdirectory if it was created.
    """
    log_total_errors_and_warnings()
    close_logging()
    move_log_file_to_results_subdirectory()


def main() -> int:
    """Program entry point and primary function."""
    searchTimer.start_timer()

    # Initialize logging - create a log file in the working directory
    initialize_logging()

    # Register the finish function to be automatically called on program exit
    atexit.register(lambda: finish())

    # Read the config.ini file variables and store them as global variables
    read_config_ini_variables()

    # Create the results subdirectory in the output folder and set the fileops global to it
    create_results_output_subdirectory()

    # Create the definitions list
    definitions = create_associated_definition_files_regex_list()

    begin_search(definitions)

    log_results_output_path()

    searchTimer.end_timer()
    searchTimer.log_execution_time()
    return 0


if __name__ == '__main__':
    sys.exit(main())