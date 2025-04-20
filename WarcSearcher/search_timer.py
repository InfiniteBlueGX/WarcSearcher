import time

from logger import *


class SearchTimer:
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.total_time = None


    def start_timer(self):
        """Initializes the timer to the current time."""
        self.start_time = time.time()


    def end_timer(self):
        """Ends the timer and calculates the total time."""

        if self.start_time is None:
            log_error("Cannot end timer - Timer has not been started.")
            return
        
        self.end_time = time.time()
        self.total_time = self.end_time - self.start_time


    def log_execution_time(self):
        """Logs the total execution time in a formatted string."""
        if self.total_time is None:
            log_error("Cannot log execution time - Timer has not been stopped.")
            return

        minutes, seconds = divmod(self.total_time, 60)
        log_info(f"Execution time: {int(minutes)} minutes and {round(seconds, 2)} seconds.")