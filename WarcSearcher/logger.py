import logging

from logger_state import LoggerState

logger_state = LoggerState()

def log_error(message):
    logging.error(f"{message}")
    logger_state.increment_error()

def log_warning(message):
    logging.warning(f"{message}")
    logger_state.increment_warning()

def log_info(message):
    logging.info(f"{message}")