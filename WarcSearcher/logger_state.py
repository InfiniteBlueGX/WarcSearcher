import logging
import os
import sys


class LoggerState:
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
        self.error_count += 1
    
    def increment_warning(self):
        self.warning_count += 1
    
    def get_final_report(self):
        return f"Errors: {self.error_count}, Warnings: {self.warning_count}"