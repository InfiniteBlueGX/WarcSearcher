import logging
import os
import sys

ERROR_COUNT = 0
WARNING_COUNT = 0

def initialize_logging_to_file():
    """Initialize logging to a output_log file in the current working directory"""
    working_directory = os.getcwd()

    if os.path.exists(f"{working_directory}/output_log.log"):
        os.remove(f"{working_directory}/output_log.log")

    file_handler = logging.FileHandler(f"{working_directory}/output_log.log")
    stream_handler = logging.StreamHandler(sys.stdout)
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[file_handler, stream_handler],
        force=True
    )
    
    # Return the file handler so it can be closed later - if not closed, the file will be in use when it needs to be moved later
    return file_handler


def close_logging_file_handler(file_handler):
    """Close the file handler and remove it from the logger"""
    logging.getLogger().removeHandler(file_handler)
    file_handler.close()


def log_warning(message):
    logging.warning(f"{message}")
    global WARNING_COUNT
    WARNING_COUNT += 1


def log_error(message):
    logging.error(f"{message}")
    global ERROR_COUNT
    ERROR_COUNT += 1


def report_errors_and_warnings():
    logging.info(f"[Errors: {ERROR_COUNT}, Warnings: {WARNING_COUNT}]")