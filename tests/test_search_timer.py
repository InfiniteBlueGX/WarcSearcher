from search_timer import SearchTimer
import time
from unittest.mock import patch

class TestSearchTimer:
    def test1(self):
        searchTimer = SearchTimer()
        assert searchTimer.start_time is None
        
    def test2(self):
        searchTimer = SearchTimer()
        searchTimer.start_timer()
        assert searchTimer.start_time is not None
        
    def test_initialization(self):
        """Test that all timer attributes are initialized to None."""
        searchTimer = SearchTimer()
        assert searchTimer.start_time is None
        assert searchTimer.end_time is None
        assert searchTimer.total_time is None
        
    def test_end_timer_with_start(self):
        """Test that end_timer correctly calculates total time when start_timer was called."""
        searchTimer = SearchTimer()
        searchTimer.start_timer()
        # Sleep briefly to ensure measurable time difference
        time.sleep(0.1)
        searchTimer.end_timer()
        assert searchTimer.end_time is not None
        assert searchTimer.total_time is not None
        assert searchTimer.total_time > 0
        
    @patch('search_timer.log_error')
    def test_end_timer_without_start(self, mock_log_error):
        """Test that end_timer logs an error when start_timer was not called."""
        searchTimer = SearchTimer()
        searchTimer.end_timer()
        mock_log_error.assert_called_once_with("Cannot end timer - Timer has not been started.")
        assert searchTimer.end_time is None
        assert searchTimer.total_time is None
        
    @patch('search_timer.log_info')
    def test_log_execution_time(self, mock_log_info):
        """Test that log_execution_time correctly formats and logs the execution time."""
        searchTimer = SearchTimer()
        searchTimer.start_timer()
        # Set a specific total_time for predictable testing
        searchTimer.end_time = searchTimer.start_time + 125.75  # 2 minutes and 5.75 seconds
        searchTimer.total_time = 125.75
        searchTimer.log_execution_time()
        mock_log_info.assert_called_once_with("Execution time: 2 minutes and 5.75 seconds.")
        
    @patch('search_timer.log_error')
    def test_log_execution_time_without_end(self, mock_log_error):
        """Test that log_execution_time logs an error when end_timer was not called."""
        searchTimer = SearchTimer()
        searchTimer.log_execution_time()
        mock_log_error.assert_called_once_with("Cannot log execution time - Timer has not been stopped.")
        
    def test_timer_accuracy(self):
        """Test that the timer measures time with reasonable accuracy."""
        searchTimer = SearchTimer()
        searchTimer.start_timer()
        sleep_time = 0.5
        time.sleep(sleep_time)
        searchTimer.end_timer()
        # Allow for small timing variations but ensure it's close to the sleep time
        assert abs(searchTimer.total_time - sleep_time) < 0.1