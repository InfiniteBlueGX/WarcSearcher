import logging

from logger_state import LoggerState

logger_state = LoggerState()


def initialize_logging():
    logger_state.initialize_logging_to_file()


def close_logging():
    logger_state.close_logging_file_handler()


def log_error(message):
    logging.error(f"{message}")
    logger_state.increment_error()


def log_warning(message):
    logging.warning(f"{message}")
    logger_state.increment_warning()


def log_info(message):
    logging.info(f"{message}")


def log_total_errors_and_warnings():
    logging.info(logger_state.get_final_report())