import unittest
from unittest.mock import patch
import config

class TestReadConfigIniVariables(unittest.TestCase):
    @patch('config.read_required_config_ini_variables')
    @patch('config.read_optional_config_ini_variables')
    @patch('config.configparser.ConfigParser.read')
    @patch('config.validate_and_get_config_ini_path')
    def test_read_config_ini_variables_success(
        self, mock_get_path, mock_parser_read, mock_read_optional, mock_read_required
    ):
        mock_get_path.return_value = 'dummy_path'
        # Should not raise, should call both required and optional readers
        config.read_config_ini_variables()
        mock_get_path.assert_called_once()
        mock_parser_read.assert_called_once_with('dummy_path')
        mock_read_required.assert_called_once()
        mock_read_optional.assert_called_once()

    @patch('config.sys.exit')
    @patch('config.log_error')
    @patch('config.read_required_config_ini_variables', side_effect=Exception('fail'))
    @patch('config.read_optional_config_ini_variables')
    @patch('config.configparser.ConfigParser.read')
    @patch('config.validate_and_get_config_ini_path')
    def test_read_config_ini_variables_exception_in_required(
        self, mock_get_path, mock_parser_read, mock_read_optional, mock_read_required, mock_log_error, mock_exit
    ):
        mock_get_path.return_value = 'dummy_path'
        config.read_config_ini_variables()
        mock_log_error.assert_called()
        mock_exit.assert_called_once()

    @patch('config.sys.exit')
    @patch('config.log_error')
    @patch('config.read_required_config_ini_variables')
    @patch('config.read_optional_config_ini_variables', side_effect=Exception('fail'))
    @patch('config.configparser.ConfigParser.read')
    @patch('config.validate_and_get_config_ini_path')
    def test_read_config_ini_variables_exception_in_optional(
        self, mock_get_path, mock_parser_read, mock_read_optional, mock_read_required, mock_log_error, mock_exit
    ):
        mock_get_path.return_value = 'dummy_path'
        config.read_config_ini_variables()
        mock_log_error.assert_called()
        mock_exit.assert_called_once()


class TestReadRequiredConfigIniVariables(unittest.TestCase):
    @patch('config.validate_and_get_warc_gz_archives_directory')
    @patch('config.validate_and_get_search_regex_definitions_directory')
    @patch('config.validate_and_get_results_output_directory')
    def test_reads_and_sets_required_variables(
        self, mock_results_dir, mock_regex_dir, mock_warc_dir
    ):
        # Arrange
        parser = unittest.mock.Mock()
        parser.get.side_effect = [
            'warc_dir', 'regex_dir', 'results_dir'
        ]
        mock_warc_dir.return_value = 'validated_warc_dir'
        mock_regex_dir.return_value = 'validated_regex_dir'
        mock_results_dir.return_value = 'validated_results_dir'
        # Act
        config.read_required_config_ini_variables(parser)
        # Assert
        parser.get.assert_any_call('REQUIRED', 'WARC_GZ_ARCHIVES_DIRECTORY')
        parser.get.assert_any_call('REQUIRED', 'SEARCH_REGEX_DEFINITIONS_DIRECTORY')
        parser.get.assert_any_call('REQUIRED', 'RESULTS_OUTPUT_DIRECTORY')
        self.assertEqual(config.settings["WARC_GZ_ARCHIVES_DIRECTORY"], 'validated_warc_dir')
        self.assertEqual(config.settings["SEARCH_REGEX_DEFINITIONS_DIRECTORY"], 'validated_regex_dir')
        self.assertEqual(config.settings["RESULTS_OUTPUT_DIRECTORY"], 'validated_results_dir')

    @patch('config.validate_and_get_warc_gz_archives_directory', side_effect=Exception('fail_warc'))
    def test_raises_if_warc_dir_invalid(self, mock_warc_dir):
        parser = unittest.mock.Mock()
        parser.get.side_effect = ['warc_dir', 'regex_dir', 'results_dir']
        with self.assertRaises(Exception) as cm:
            config.read_required_config_ini_variables(parser)
        self.assertIn('fail_warc', str(cm.exception))

    @patch('config.validate_and_get_warc_gz_archives_directory')
    @patch('config.validate_and_get_search_regex_definitions_directory', side_effect=Exception('fail_regex'))
    def test_raises_if_regex_dir_invalid(self, mock_regex_dir, mock_warc_dir):
        parser = unittest.mock.Mock()
        parser.get.side_effect = ['warc_dir', 'regex_dir', 'results_dir']
        mock_warc_dir.return_value = 'validated_warc_dir'
        with self.assertRaises(Exception) as cm:
            config.read_required_config_ini_variables(parser)
        self.assertIn('fail_regex', str(cm.exception))

    @patch('config.validate_and_get_warc_gz_archives_directory')
    @patch('config.validate_and_get_search_regex_definitions_directory')
    @patch('config.validate_and_get_results_output_directory', side_effect=Exception('fail_results'))
    def test_raises_if_results_dir_invalid(self, mock_results_dir, mock_regex_dir, mock_warc_dir):
        parser = unittest.mock.Mock()
        parser.get.side_effect = ['warc_dir', 'regex_dir', 'results_dir']
        mock_warc_dir.return_value = 'validated_warc_dir'
        mock_regex_dir.return_value = 'validated_regex_dir'
        with self.assertRaises(Exception) as cm:
            config.read_required_config_ini_variables(parser)
        self.assertIn('fail_results', str(cm.exception))


class TestReadOptionalConfigIniVariables(unittest.TestCase):
    @patch('config.validate_and_get_max_concurrent_search_processes')
    @patch('config.validate_and_get_max_ram_usage_percent')
    def test_reads_and_sets_optional_variables(
        self, mock_validate_ram, mock_validate_concurrent
    ):
        parser = unittest.mock.Mock()
        parser.getboolean.side_effect = [True, False]
        parser.get.side_effect = ['4', '80']
        mock_validate_concurrent.return_value = 4
        mock_validate_ram.return_value = 80

        config.read_optional_config_ini_variables(parser)

        parser.getboolean.assert_any_call('OPTIONAL', 'ZIP_FILES_WITH_MATCHES')
        parser.get.assert_any_call('OPTIONAL', 'MAX_CONCURRENT_SEARCH_PROCESSES')
        parser.get.assert_any_call('OPTIONAL', 'MAX_RAM_USAGE_PERCENT')
        parser.getboolean.assert_any_call('OPTIONAL', 'SEARCH_BINARY_FILES')
        self.assertEqual(config.settings["ZIP_FILES_WITH_MATCHES"], True)
        self.assertEqual(config.settings["MAX_CONCURRENT_SEARCH_PROCESSES"], 4)
        self.assertEqual(config.settings["MAX_RAM_USAGE_PERCENT"], 80)
        self.assertEqual(config.settings["SEARCH_BINARY_FILES"], False)

    @patch('config.validate_and_get_max_concurrent_search_processes')
    @patch('config.validate_and_get_max_ram_usage_percent')
    def test_raises_if_missing_optional_keys(
        self, mock_validate_ram, mock_validate_concurrent
    ):
        parser = unittest.mock.Mock()
        parser.getboolean.side_effect = KeyError('ZIP_FILES_WITH_MATCHES')
        with self.assertRaises(KeyError):
            config.read_optional_config_ini_variables(parser)

    @patch('config.validate_and_get_max_concurrent_search_processes', side_effect=Exception('fail_concurrent'))
    @patch('config.validate_and_get_max_ram_usage_percent')
    def test_raises_if_concurrent_invalid(
        self, mock_validate_ram, mock_validate_concurrent
    ):
        parser = unittest.mock.Mock()
        parser.getboolean.side_effect = [True, False]
        parser.get.side_effect = ['bad', '80']
        mock_validate_ram.return_value = 80
        with self.assertRaises(Exception) as cm:
            config.read_optional_config_ini_variables(parser)
        self.assertIn('fail_concurrent', str(cm.exception))

    @patch('config.validate_and_get_max_concurrent_search_processes')
    @patch('config.validate_and_get_max_ram_usage_percent', side_effect=Exception('fail_ram'))
    def test_raises_if_ram_invalid(
        self, mock_validate_ram, mock_validate_concurrent
    ):
        parser = unittest.mock.Mock()
        parser.getboolean.side_effect = [True, False]
        parser.get.side_effect = ['4', 'bad']
        mock_validate_concurrent.return_value = 4
        with self.assertRaises(Exception) as cm:
            config.read_optional_config_ini_variables(parser)
        self.assertIn('fail_ram', str(cm.exception))


class TestValidateAndGetConfigIniPath(unittest.TestCase):
    @patch('config.os.path.isfile')
    def test_returns_current_directory_path(self, mock_isfile):
        # Plan:
        # - Simulate config.ini exists in current directory
        # - Should return 'config.ini'
        mock_isfile.side_effect = lambda path: path == 'config.ini'
        result = config.validate_and_get_config_ini_path()
        self.assertEqual(result, 'config.ini')
        mock_isfile.assert_any_call('config.ini')
        # Should not check parent if found in current
        self.assertFalse(mock_isfile.call_args_list[-1][0][0] == '../config.ini' and mock_isfile.call_count > 1)

    @patch('config.os.path.isfile')
    def test_returns_parent_directory_path(self, mock_isfile):
        # Plan:
        # - Simulate config.ini does not exist in current, but exists in parent
        # - Should return '../config.ini'
        def isfile_side_effect(path):
            return path == '../config.ini'
        mock_isfile.side_effect = isfile_side_effect
        result = config.validate_and_get_config_ini_path()
        self.assertEqual(result, '../config.ini')
        self.assertEqual(mock_isfile.call_args_list[0][0][0], 'config.ini')
        self.assertEqual(mock_isfile.call_args_list[1][0][0], '../config.ini')

    @patch('config.sys.exit')
    @patch('config.log_error')
    @patch('config.os.path.isfile', return_value=False)
    def test_exits_and_logs_if_not_found(self, mock_isfile, mock_log_error, mock_exit):
        # Plan:
        # - Simulate config.ini does not exist in either location
        # - Should call log_error and sys.exit, not return
        config.validate_and_get_config_ini_path()
        mock_log_error.assert_called_once()
        mock_exit.assert_called_once()


class TestValidateAndGetWarcGzArchivesDirectory(unittest.TestCase):
    @patch('config.os.path.exists', return_value=True)
    @patch('config.glob.glob', return_value=['file1.gz', 'file2.gz'])
    def test_returns_directory_when_valid(self, mock_glob, mock_exists):
        result = config.validate_and_get_warc_gz_archives_directory('some_dir')
        self.assertEqual(result, 'some_dir')
        mock_exists.assert_called_once_with('some_dir')
        mock_glob.assert_called_once_with('some_dir/*.gz')

    @patch('config.sys.exit')
    @patch('config.log_error')
    @patch('config.os.path.exists', return_value=False)
    def test_exits_when_directory_does_not_exist(self, mock_exists, mock_log_error, mock_exit):
        config.validate_and_get_warc_gz_archives_directory('missing_dir')
        mock_log_error.assert_called_once()
        mock_exit.assert_called_once()

    @patch('config.sys.exit')
    @patch('config.log_error')
    @patch('config.os.path.exists', return_value=True)
    @patch('config.glob.glob', return_value=[])
    def test_exits_when_no_gz_files(self, mock_glob, mock_exists, mock_log_error, mock_exit):
        config.validate_and_get_warc_gz_archives_directory('empty_dir')
        mock_exists.assert_called_once_with('empty_dir')
        mock_glob.assert_called_once_with('empty_dir/*.gz')
        mock_log_error.assert_called_once()
        mock_exit.assert_called_once()


class TestValidateAndGetSearchRegexDefinitionsDirectory(unittest.TestCase):
    @patch('config.os.path.exists', return_value=True)
    @patch('config.glob.glob', return_value=['regex1.txt', 'regex2.txt'])
    def test_returns_directory_when_valid(self, mock_glob, mock_exists):
        result = config.validate_and_get_search_regex_definitions_directory('regex_dir')
        self.assertEqual(result, 'regex_dir')
        mock_exists.assert_called_once_with('regex_dir')
        mock_glob.assert_called_once_with('regex_dir/*.txt')

    @patch('config.sys.exit')
    @patch('config.log_error')
    @patch('config.os.path.exists', return_value=False)
    def test_exits_when_directory_does_not_exist(self, mock_exists, mock_log_error, mock_exit):
        config.validate_and_get_search_regex_definitions_directory('missing_dir')
        mock_log_error.assert_called_once_with(
            "Directory containing the regex definition .txt files to search with does not exist: missing_dir. Exiting."
        )
        mock_exit.assert_called_once()

    @patch('config.sys.exit')
    @patch('config.log_error')
    @patch('config.os.path.exists', return_value=True)
    @patch('config.glob.glob', return_value=[])
    def test_exits_when_no_txt_files(self, mock_glob, mock_exists, mock_log_error, mock_exit):
        config.validate_and_get_search_regex_definitions_directory('empty_dir')
        mock_exists.assert_called_once_with('empty_dir')
        mock_glob.assert_called_once_with('empty_dir/*.txt')
        mock_log_error.assert_called_once_with(
            "Directory that should contain the regex definition .txt files to search with does not contain any: empty_dir. Exiting."
        )
        mock_exit.assert_called_once()


class TestValidateAndGetResultsOutputDirectory(unittest.TestCase):
    @patch('config.os.path.exists', return_value=True)
    def test_returns_directory_when_valid(self, mock_exists):
        result = config.validate_and_get_results_output_directory('results_dir')
        self.assertEqual(result, 'results_dir')
        mock_exists.assert_called_once_with('results_dir')

    @patch('config.sys.exit')
    @patch('config.log_error')
    @patch('config.os.path.exists', return_value=False)
    def test_exits_when_directory_does_not_exist(self, mock_exists, mock_log_error, mock_exit):
        config.validate_and_get_results_output_directory('missing_results_dir')
        mock_log_error.assert_called_once_with(
            "Directory to output the search results to does not exist: missing_results_dir. Exiting."
        )
        mock_exit.assert_called_once()


class TestValidateAndGetMaxConcurrentSearchProcesses(unittest.TestCase):
    @patch('config.os.cpu_count', return_value=8)
    def test_returns_int_when_valid(self, mock_cpu_count):
        # Should return the int value if within range
        self.assertEqual(config.validate_and_get_max_concurrent_search_processes('4'), 4)
        self.assertEqual(config.validate_and_get_max_concurrent_search_processes('8'), 8)

    @patch('config.os.cpu_count', return_value=8)
    def test_returns_cpu_count_when_none(self, mock_cpu_count):
        # Should return cpu_count if 'none'
        self.assertEqual(config.validate_and_get_max_concurrent_search_processes('none'), 8)

    @patch('config.os.cpu_count', return_value=8)
    @patch('config.log_warning')
    def test_returns_cpu_count_and_warns_on_invalid_string(self, mock_log_warning, mock_cpu_count):
        # Should warn and return cpu_count if not an int
        result = config.validate_and_get_max_concurrent_search_processes('notanumber')
        self.assertEqual(result, 8)
        mock_log_warning.assert_called_once()
    
    @patch('config.os.cpu_count', return_value=8)
    @patch('config.log_warning')
    def test_returns_cpu_count_and_warns_on_zero(self, mock_log_warning, mock_cpu_count):
        # Should warn and return cpu_count if 0
        result = config.validate_and_get_max_concurrent_search_processes('0')
        self.assertEqual(result, 8)
        mock_log_warning.assert_called_once()

    @patch('config.os.cpu_count', return_value=8)
    @patch('config.log_warning')
    def test_returns_cpu_count_and_warns_on_negative(self, mock_log_warning, mock_cpu_count):
        # Should warn and return cpu_count if negative
        result = config.validate_and_get_max_concurrent_search_processes('-2')
        self.assertEqual(result, 8)
        mock_log_warning.assert_called_once()

    @patch('config.os.cpu_count', return_value=8)
    @patch('config.log_warning')
    def test_returns_cpu_count_and_warns_on_too_large(self, mock_log_warning, mock_cpu_count):
        # Should warn and return cpu_count if value > cpu_count
        result = config.validate_and_get_max_concurrent_search_processes('100')
        self.assertEqual(result, 8)
        mock_log_warning.assert_called_once()


class TestValidateAndGetMaxRamUsagePercent(unittest.TestCase):
    @patch('config.log_warning')
    def test_returns_int_when_valid(self, mock_log_warning):
        # Should return the int value if within range
        self.assertEqual(config.validate_and_get_max_ram_usage_percent('50'), 50)
        self.assertEqual(config.validate_and_get_max_ram_usage_percent('100'), 100)
        self.assertEqual(config.validate_and_get_max_ram_usage_percent('1'), 1)
        mock_log_warning.assert_not_called()

    @patch('config.log_warning')
    def test_returns_100_when_none(self, mock_log_warning):
        # Should return 100 if 'none'
        self.assertEqual(config.validate_and_get_max_ram_usage_percent('none'), 100)
        mock_log_warning.assert_not_called()

    @patch('config.log_warning')
    def test_returns_90_and_warns_on_invalid_string(self, mock_log_warning):
        # Should warn and return 90 if not an int
        result = config.validate_and_get_max_ram_usage_percent('notanumber')
        self.assertEqual(result, 90)
        mock_log_warning.assert_called_once()

    @patch('config.log_warning')
    def test_returns_90_and_warns_on_zero(self, mock_log_warning):
        # Should warn and return 90 if 0
        result = config.validate_and_get_max_ram_usage_percent('0')
        self.assertEqual(result, 90)
        mock_log_warning.assert_called_once()

    @patch('config.log_warning')
    def test_returns_90_and_warns_on_negative(self, mock_log_warning):
        # Should warn and return 90 if negative
        result = config.validate_and_get_max_ram_usage_percent('-5')
        self.assertEqual(result, 90)
        mock_log_warning.assert_called_once()

    @patch('config.log_warning')
    def test_returns_90_and_warns_on_too_large(self, mock_log_warning):
        # Should warn and return 90 if value > 100
        result = config.validate_and_get_max_ram_usage_percent('101')
        self.assertEqual(result, 90)
        mock_log_warning.assert_called_once()