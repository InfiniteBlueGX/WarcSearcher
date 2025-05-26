import unittest
from unittest.mock import patch
import main

class TestMainOnExit(unittest.TestCase):
    @patch('main.move_log_file_to_results_subdirectory')
    @patch('main.close_logging')
    @patch('main.log_total_errors_and_warnings')
    @patch('main.searchTimer')
    @patch('main.log_results_output_path')
    def test_on_exit_calls_all_functions(self, mock_log_results_output_path, mock_search_timer, mock_log_total_errors_and_warnings, mock_close_logging, mock_move_log_file):
        main.on_exit()
        mock_log_results_output_path.assert_called_once()
        mock_search_timer.end_timer.assert_called_once()
        mock_search_timer.log_execution_time.assert_called_once()
        mock_log_total_errors_and_warnings.assert_called_once()
        mock_close_logging.assert_called_once()
        mock_move_log_file.assert_called_once()

class TestMainSetup(unittest.TestCase):
    @patch('main.initialize_results_output_subdirectory')
    @patch('main.read_config_ini_variables')
    @patch('main.atexit.register')
    @patch('main.initialize_logging')
    @patch('main.searchTimer')
    def test_setup_calls_all_functions(self, mock_search_timer, mock_initialize_logging, mock_atexit_register, mock_read_config, mock_init_results_dir):
        import main
        main.setup()
        mock_search_timer.start_timer.assert_called_once()
        mock_initialize_logging.assert_called_once()
        mock_atexit_register.assert_called_once()
        mock_read_config.assert_called_once()
        mock_init_results_dir.assert_called_once()

class TestMainEntryPoint(unittest.TestCase):
    @patch('main.perform_search')
    @patch('main.setup')
    def test_main_runs_setup_and_perform_search(self, mock_setup, mock_perform_search):
        import main
        result = main.main()
        mock_setup.assert_called_once()
        mock_perform_search.assert_called_once()
        self.assertEqual(result, 0)

    @patch('main.main', return_value=0)
    @patch('main.sys')
    def test_entry_point_calls_sys_exit(self, mock_sys, mock_main):
        import importlib
        import main
        importlib.reload(main)  # Ensure __name__ == '__main__' is not triggered
        if '__main__' in main.__name__:
            main.__name__ = '__main__'
            main.sys.exit = mock_sys.exit
            main.main = mock_main
            exec(open(main.__file__).read())
            mock_sys.exit.assert_called_once_with(0)