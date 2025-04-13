import logging
import os
import sys

class WarcSearcherLogger:
    error_count = 0
    warning_count = 0
    file_handler = None
    
    @classmethod
    def initialize_logging_to_file(cls):
        """Initialize logging to a output_log file in the current working directory"""
        working_directory = os.getcwd()
        log_path = f"{working_directory}/output_log.log"
        
        if os.path.exists(log_path):
            os.remove(log_path)
            
        cls.file_handler = logging.FileHandler(log_path)
        stream_handler = logging.StreamHandler(sys.stdout)
        
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[cls.file_handler, stream_handler],
            force=True
        )
        
        return cls.file_handler
    
    @classmethod
    def close_logging_file_handler(cls):
        """Close the file handler and remove it from the logger"""
        if cls.file_handler:
            logging.getLogger().removeHandler(cls.file_handler)
            cls.file_handler.close()
            cls.file_handler = None

    @classmethod
    def log_info(cls, message):
        logging.info(f"{message}")
    
    @classmethod
    def log_warning(cls, message):
        logging.warning(f"{message}")
        cls.warning_count += 1
    
    @classmethod
    def log_error(cls, message):
        logging.error(f"{message}")
        cls.error_count += 1
    
    @classmethod
    def report_errors_and_warnings(cls):
        logging.info(f"[Errors: {cls.error_count}, Warnings: {cls.warning_count}]")
    
    @classmethod
    def reset_counters(cls):
        """Reset error and warning counters to zero"""
        cls.error_count = 0
        cls.warning_count = 0