import time

import logger


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
        self.end_time = time.time()
        self.total_time = self.end_time - self.start_time


    def log_execution_time(self):
        """Logs the total execution time in a formatted string."""
        if self.total_time is None:
            return logger.log_error("Timer has not been started or stopped.")

        minutes, seconds = divmod(self.total_time, 60)
        logger.log_info(f"Execution time: {int(minutes)} minutes and {round(seconds, 2)} seconds.")