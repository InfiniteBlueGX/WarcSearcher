import logging
import os
import sys

class Logger:
    def __init__(self):
        self.error_count = 0
        self.warning_count = 0
        self.file_handler = None
    
    def initialize_logging_to_file(self):
        """Initialize logging to a output_log.log file in the current working directory."""
        working_directory = os.getcwd()
        log_path = f"{working_directory}/output_log.log"
        if os.path.exists(log_path):
            os.remove(log_path)
        self.file_handler = logging.FileHandler(log_path)
        stream_handler = logging.StreamHandler(sys.stdout)
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[self.file_handler, stream_handler],
            force=True
        )
    
    def close_logging_file_handler(self):
        """Close the file handler and remove it from the logger."""
        if self.file_handler:
            logging.getLogger().removeHandler(self.file_handler)
            self.file_handler.close()
            self.file_handler = None
    
    def increment_error(self):
        """Increment the error count and log the error."""
        self.error_count += 1
    
    def increment_warning(self):
        """Increment the warning count and log the warning."""
        self.warning_count += 1
    
    def get_final_report(self):
        """Return a summary of the total errors and warnings."""
        return f"Errors: {self.error_count}, Warnings: {self.warning_count}"

logger = Logger()

def initialize_logging():
    logger.initialize_logging_to_file()

def close_logging():
    logger.close_logging_file_handler()

def log_error(message):
    logging.error(f"{message}")
    logger.increment_error()

def log_warning(message):
    logging.warning(f"{message}")
    logger.increment_warning()

def log_info(message):
    logging.info(f"{message}")

def log_total_errors_and_warnings():
    logging.info(logger.get_final_report())