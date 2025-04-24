import atexit

from config import read_config_ini_variables
from results import *
from search import start_search
from search_timer import SearchTimer

searchTimer = SearchTimer()


def setup():
    """Initializes logging, registers exit handler, reads configuration variables, and creates the results directory."""
    searchTimer.start_timer()
    initialize_logging()
    atexit.register(lambda: on_exit())
    read_config_ini_variables()
    initialize_results_output_subdirectory()


def on_exit():
    """
    Function to be called on program exit. Logs the results path, the execution time, and the total errors/warnings. 
    Logging is then closed and the log file is moved to the results directory if one exists.
    """
    log_results_output_path()
    searchTimer.end_timer()
    searchTimer.log_execution_time()
    log_total_errors_and_warnings()
    close_logging()
    move_log_file_to_results_subdirectory()


def main() -> int:
    """Program entry point."""
    setup()
    start_search()

    return 0


if __name__ == '__main__':
    sys.exit(main())