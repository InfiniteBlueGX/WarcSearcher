import logging

from logger_state import LoggerState

logger_state = LoggerState()

def log_error(message):
    logging.error(f"ERROR: {message}")
    logger_state.increment_error()

def log_warning(message):
    logging.warning(f"WARNING: {message}")
    logger_state.increment_warning()

def log_info(message):
    logging.info(f"{message}")