import os
import pytest
from logger import Logger, initialize_logging, close_logging, log_error, log_warning, log_info, log_total_errors_and_warnings

@pytest.fixture
def logger_instance():
    logger = Logger()
    yield logger

    if logger.file_handler:
        logger.close_logging_file_handler()

    log_path = os.path.join(os.getcwd(), "log.log")
    if os.path.exists(log_path):
        os.remove(log_path)

def test_logger_initialization(logger_instance):
    assert logger_instance.error_count == 0
    assert logger_instance.warning_count == 0
    assert logger_instance.file_handler is None

def test_initialize_logging_to_file(logger_instance):
    logger_instance.initialize_logging_to_file()
    assert logger_instance.file_handler is not None
    assert os.path.exists(os.path.join(os.getcwd(), "log.log"))

def test_close_logging_file_handler(logger_instance):
    logger_instance.initialize_logging_to_file()
    assert logger_instance.file_handler is not None
    logger_instance.close_logging_file_handler()
    assert logger_instance.file_handler is None

def test_increment_error(logger_instance):
    initial_count = logger_instance.error_count
    logger_instance.increment_error()
    assert logger_instance.error_count == initial_count + 1

def test_increment_warning(logger_instance):
    initial_count = logger_instance.warning_count
    logger_instance.increment_warning()
    assert logger_instance.warning_count == initial_count + 1

def test_get_total_errors_and_warnings(logger_instance):
    logger_instance.increment_error()
    logger_instance.increment_warning()
    logger_instance.increment_error()
    report = logger_instance.get_total_errors_and_warnings()
    assert report == "Errors: 2, Warnings: 1"

def test_log_file_content():
    initialize_logging()
    
    test_error = "Test error message"
    test_warning = "Test warning message"
    test_info = "Test info message"
    
    log_error(test_error)
    log_warning(test_warning)
    log_info(test_info)
    
    close_logging()
    
    # Read log file and check contents
    log_path = os.path.join(os.getcwd(), "log.log")
    with open(log_path, 'r') as log_file:
        content = log_file.read()
        assert test_error in content
        assert test_warning in content
        assert test_info in content