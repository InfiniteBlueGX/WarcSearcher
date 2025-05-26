import os
import time
from io import StringIO
import sys
import pytest
import search
import zipfile

# A fake queue that always returns the same value
class FakeQueue:
    def __init__(self, sizes):
        self.sizes = sizes
        self.index = 0

    def qsize(self):
        if self.index < len(self.sizes):
            return self.sizes[self.index]
        return 0

# A fake queue that simulates decreasing values correctly for print_remaining_search_queue_items().
# It returns the same size for two consecutive calls then moves to the next value.
class FakeQueueSim:
    def __init__(self, sizes):
        self.sizes = sizes
        self.index = 0
        self.toggle = False  # False means not yet returned this iteration's value twice

    def qsize(self):
        if self.index < len(self.sizes):
            current = self.sizes[self.index]
            if self.toggle:
                # After the second call, move to the next value.
                self.toggle = False
                self.index += 1
            else:
                self.toggle = True
            return current
        return 0

# Override sleep to avoid delays during tests
@pytest.fixture(autouse=True)
def fast_sleep(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda s: None)
    yield

def test_print_remaining_queue_empty(capfd):
    # Set SEARCH_QUEUE to a fake queue that is already empty
    search.SEARCH_QUEUE = FakeQueue([0])
    search.print_remaining_search_queue_items()
    captured = capfd.readouterr().out
    # Verify that the final message with 0 remains is printed
    assert "Remaining records to search: 0" in captured

def test_print_remaining_queue_decreasing(capfd):
    # Simulate a queue that decreases from 3 to 0:
    # For each iteration, the value is returned twice (once for the loop condition, once for the print)
    search.SEARCH_QUEUE = FakeQueueSim([3, 2, 1, 0])
    search.print_remaining_search_queue_items()
    captured = capfd.readouterr().out
    # Verify that output contains the expected updates:
    assert "Remaining records to search: 3" in captured
    assert "Remaining records to search: 2" in captured
    assert "Remaining records to search: 1" in captured
    assert "Remaining records to search: 0" in captured
    
def test_signal_worker_processes_to_stop_puts_none(monkeypatch):
    class FakeQueue:
        def __init__(self):
            self.items = []
        def put(self, value):
            self.items.append(value)

    fake_queue = FakeQueue()
    monkeypatch.setattr(search, "SEARCH_QUEUE", fake_queue)

    search.signal_worker_processes_to_stop(3)
    assert fake_queue.items == [None, None, None]

def test_signal_worker_processes_to_stop_zero(monkeypatch):
    class FakeQueue:
        def __init__(self):
            self.items = []
        def put(self, value):
            self.items.append(value)

    fake_queue = FakeQueue()
    monkeypatch.setattr(search, "SEARCH_QUEUE", fake_queue)

    search.signal_worker_processes_to_stop(0)
    assert fake_queue.items == []

def test_signal_worker_processes_to_stop_one(monkeypatch):
    class FakeQueue:
        def __init__(self):
            self.items = []
        def put(self, value):
            self.items.append(value)

    fake_queue = FakeQueue()
    monkeypatch.setattr(search, "SEARCH_QUEUE", fake_queue)

    search.signal_worker_processes_to_stop(1)
    assert fake_queue.items == [None]

def test_perform_search_basic(monkeypatch):
    # Plan:
    # - Patch all dependencies used in perform_search
    # - Simulate config/settings, glob, Manager, and all called functions
    # - Check that the correct sequence of calls is made

    called = {}

    # Fake config/settings
    class FakeConfig:
        settings = {
            "WARC_GZ_ARCHIVES_DIRECTORY": "/fake/dir",
            "ZIP_FILES_WITH_MATCHES": False,
            "MAX_CONCURRENT_SEARCH_PROCESSES": 2,
        }
    monkeypatch.setattr("search.config", FakeConfig)

    # Fake glob
    monkeypatch.setattr("search.glob", type("FakeGlob", (), {"glob": staticmethod(lambda pattern: ["file1.gz", "file2.gz"])}))

    # Fake Manager and its Queue
    class FakeQueue:
        def __init__(self): pass
    class FakeManager:
        def Queue(self): called["queue"] = True; return FakeQueue()
    monkeypatch.setattr("search.Manager", FakeManager)

    # Fake create_result_files_associated_with_regexes_dict
    monkeypatch.setattr("search.create_result_files_associated_with_regexes_dict", lambda: {"result1.txt": "regex1"})

    # Fake write_result_files_headers
    monkeypatch.setattr("search.write_result_files_headers", lambda d: called.setdefault("write_headers", True))

    # Fake create_result_files_write_locks_dict
    monkeypatch.setattr("search.create_result_files_write_locks_dict", lambda m, k: {"result1.txt": object()})

    # Fake initiate_search_worker_processes
    def fake_initiate_search_worker_processes(files, dct, locks):
        called["initiate_workers"] = (files, dct, locks)
    monkeypatch.setattr("search.initiate_search_worker_processes", fake_initiate_search_worker_processes)

    # Fake log_info
    monkeypatch.setattr("search.log_info", lambda msg: called.setdefault("log_info", msg))

    # Fake finalize_results_zip_archives (should not be called in this test)
    monkeypatch.setattr("search.finalize_results_zip_archives", lambda keys: called.setdefault("finalize_zip", True))

    # Run
    search.perform_search()

    # Assert
    assert called["queue"]
    assert called["write_headers"]
    assert "initiate_workers" in called
    assert called["log_info"] == "Finished searching."
    assert "finalize_zip" not in called  # ZIP_FILES_WITH_MATCHES is False

def test_perform_search_with_zip(monkeypatch):
    # Plan:
    # - Patch config to enable ZIP_FILES_WITH_MATCHES
    # - Patch finalize_results_zip_archives to record call

    called = {}

    class FakeConfig:
        settings = {
            "WARC_GZ_ARCHIVES_DIRECTORY": "/fake/dir",
            "ZIP_FILES_WITH_MATCHES": True,
            "MAX_CONCURRENT_SEARCH_PROCESSES": 2,
        }
    monkeypatch.setattr("search.config", FakeConfig)
    monkeypatch.setattr("search.glob", type("FakeGlob", (), {"glob": staticmethod(lambda pattern: ["file1.gz"])}))
    class FakeQueue: pass
    class FakeManager:
        def Queue(self): return FakeQueue()
    monkeypatch.setattr("search.Manager", FakeManager)
    monkeypatch.setattr("search.create_result_files_associated_with_regexes_dict", lambda: {"result1.txt": "regex1"})
    monkeypatch.setattr("search.write_result_files_headers", lambda d: None)
    monkeypatch.setattr("search.create_result_files_write_locks_dict", lambda m, k: {"result1.txt": object()})
    monkeypatch.setattr("search.initiate_search_worker_processes", lambda files, dct, locks: None)
    monkeypatch.setattr("search.log_info", lambda msg: None)
    def fake_finalize(keys):
        called["finalize_zip"] = list(keys)
    monkeypatch.setattr("search.finalize_results_zip_archives", fake_finalize)

    search.perform_search()
    assert called["finalize_zip"] == ["result1.txt"]

def test_perform_search_empty_files(monkeypatch):
    # Plan:
    # - Simulate glob returning empty list
    # - Ensure initiate_search_worker_processes is still called with empty list

    called = {}

    class FakeConfig:
        settings = {
            "WARC_GZ_ARCHIVES_DIRECTORY": "/fake/dir",
            "ZIP_FILES_WITH_MATCHES": False,
            "MAX_CONCURRENT_SEARCH_PROCESSES": 2,
        }
    monkeypatch.setattr("search.config", FakeConfig)
    monkeypatch.setattr("search.glob", type("FakeGlob", (), {"glob": staticmethod(lambda pattern: [])}))
    class FakeQueue: pass
    class FakeManager:
        def Queue(self): return FakeQueue()
    monkeypatch.setattr("search.Manager", FakeManager)
    monkeypatch.setattr("search.create_result_files_associated_with_regexes_dict", lambda: {"result1.txt": "regex1"})
    monkeypatch.setattr("search.write_result_files_headers", lambda d: None)
    monkeypatch.setattr("search.create_result_files_write_locks_dict", lambda m, k: {"result1.txt": object()})
    def fake_initiate(files, dct, locks):
        called["files"] = files
    monkeypatch.setattr("search.initiate_search_worker_processes", fake_initiate)
    monkeypatch.setattr("search.log_info", lambda msg: None)
    monkeypatch.setattr("search.finalize_results_zip_archives", lambda keys: None)

    search.perform_search()
    assert called["files"] == []

def test_initiate_search_worker_processes_basic(monkeypatch):
    # Plan:
    # - Patch dependencies: calculate_max_search_worker_processes, log_info, ProcessPoolExecutor, initiate_warc_gz_read_threads,
    #   signal_worker_processes_to_stop, print_remaining_search_queue_items, wait
    # - Simulate a basic run and verify the correct sequence of calls and arguments

    called = {}

    # Patch calculate_max_search_worker_processes to return 2
    monkeypatch.setattr("search.calculate_max_search_worker_processes", lambda: 2)

    # Patch log_info to record messages
    monkeypatch.setattr("search.log_info", lambda msg: called.setdefault("log_info", []).append(msg))

    # Patch ProcessPoolExecutor to simulate context manager and submit
    class FakeFuture:
        pass
    class FakeExecutor:
        def __init__(self, **kwargs):
            called["executor_init"] = kwargs
        def __enter__(self):
            called["executor_enter"] = True
            return self
        def __exit__(self, exc_type, exc_val, exc_tb):
            called["executor_exit"] = True
        def submit(self, *args, **kwargs):
            called.setdefault("submit_calls", []).append((args, kwargs))
            return FakeFuture()
    monkeypatch.setattr("search.ProcessPoolExecutor", FakeExecutor)

    # Patch initiate_warc_gz_read_threads
    monkeypatch.setattr("search.initiate_warc_gz_read_threads", lambda files: called.setdefault("read_threads", files))

    # Patch signal_worker_processes_to_stop
    monkeypatch.setattr("search.signal_worker_processes_to_stop", lambda n: called.setdefault("signal_workers", n))

    # Patch print_remaining_search_queue_items
    monkeypatch.setattr("search.print_remaining_search_queue_items", lambda: called.setdefault("print_remaining", True))

    # Patch wait
    monkeypatch.setattr("search.wait", lambda futures: called.setdefault("waited", True))

    # Prepare dummy args
    gz_files_list = ["file1.gz", "file2.gz"]
    results_and_regexes_dict = {"result1.txt": "regex1"}
    result_files_write_locks_dict = {"result1.txt": object()}

    # Patch SEARCH_QUEUE global
    monkeypatch.setattr("search.SEARCH_QUEUE", "dummy_queue")

    # Run
    search.initiate_search_worker_processes(gz_files_list, results_and_regexes_dict, result_files_write_locks_dict)

    # Assert
    assert called["executor_init"] == {"max_workers": 2}
    assert called["executor_enter"]
    assert len(called["submit_calls"]) == 2
    for args, kwargs in called["submit_calls"]:
        assert args[0] == search.search_worker_process
        assert args[1] == "dummy_queue"
        assert args[2] == results_and_regexes_dict
        assert args[3] == result_files_write_locks_dict
        # args[4] is config.settings["ZIP_FILES_WITH_MATCHES"], not checked here
    assert called["read_threads"] == gz_files_list
    assert "All records read from the WARC.gz files." in "".join(called["log_info"])
    assert called["signal_workers"] == 2
    assert called["print_remaining"] is True
    assert called["waited"] is True

def test_initiate_search_worker_processes_zero_workers(monkeypatch):
    # Plan:
    # - Patch calculate_max_search_worker_processes to return 0 (should still create at least one worker)
    # - Ensure ProcessPoolExecutor is called with max_workers=0

    called = {}

    monkeypatch.setattr("search.calculate_max_search_worker_processes", lambda: 0)
    monkeypatch.setattr("search.log_info", lambda msg: None)
    class FakeExecutor:
        def __init__(self, **kwargs):
            called["max_workers"] = kwargs.get("max_workers")
        def __enter__(self): return self
        def __exit__(self, exc_type, exc_val, exc_tb): pass
        def submit(self, *args, **kwargs): return object()
    monkeypatch.setattr("search.ProcessPoolExecutor", FakeExecutor)
    monkeypatch.setattr("search.initiate_warc_gz_read_threads", lambda files: None)
    monkeypatch.setattr("search.signal_worker_processes_to_stop", lambda n: None)
    monkeypatch.setattr("search.print_remaining_search_queue_items", lambda: None)
    monkeypatch.setattr("search.wait", lambda futures: None)
    monkeypatch.setattr("search.SEARCH_QUEUE", "dummy_queue")

    search.initiate_search_worker_processes([], {}, {})

    assert called["max_workers"] == 0

def test_initiate_search_worker_processes_calls_all_steps(monkeypatch):
    # Plan:
    # - Patch all steps to set flags when called
    # - Ensure all steps are called in order

    steps = []

    monkeypatch.setattr("search.calculate_max_search_worker_processes", lambda: 1)
    monkeypatch.setattr("search.log_info", lambda msg: steps.append("log_info"))
    class FakeExecutor:
        def __init__(self, **kwargs):  # Accept any kwargs, e.g., max_workers
            pass
        def __enter__(self): steps.append("executor_enter"); return self
        def __exit__(self, exc_type, exc_val, exc_tb): steps.append("executor_exit")
        def submit(self, *args, **kwargs): steps.append("submit"); return object()
    monkeypatch.setattr("search.ProcessPoolExecutor", FakeExecutor)
    monkeypatch.setattr("search.initiate_warc_gz_read_threads", lambda files: steps.append("read_threads"))
    monkeypatch.setattr("search.signal_worker_processes_to_stop", lambda n: steps.append("signal_workers"))
    monkeypatch.setattr("search.print_remaining_search_queue_items", lambda: steps.append("print_remaining"))
    monkeypatch.setattr("search.wait", lambda futures: steps.append("wait"))
    monkeypatch.setattr("search.SEARCH_QUEUE", "dummy_queue")

    search.initiate_search_worker_processes(["f1"], {"r": "re"}, {"r": object()})

    # Check that all steps are present in the correct order
    assert steps == [
        "log_info",
        "executor_enter",
        "submit",
        "read_threads",
        "log_info",
        "signal_workers",
        "print_remaining",
        "wait",
        "executor_exit"
    ]

def test_calculate_max_search_worker_processes_gt_1(monkeypatch):
    # Plan:
    # - Patch config.settings["MAX_CONCURRENT_SEARCH_PROCESSES"] to a value > 1
    # - Should return MAX_CONCURRENT_SEARCH_PROCESSES - 1
    class FakeConfig:
        settings = {"MAX_CONCURRENT_SEARCH_PROCESSES": 5}
    monkeypatch.setattr("search.config", FakeConfig)
    assert search.calculate_max_search_worker_processes() == 4

def test_calculate_max_search_worker_processes_eq_1(monkeypatch):
    # Plan:
    # - Patch config.settings["MAX_CONCURRENT_SEARCH_PROCESSES"] to 1
    # - Should return 1
    class FakeConfig:
        settings = {"MAX_CONCURRENT_SEARCH_PROCESSES": 1}
    monkeypatch.setattr("search.config", FakeConfig)
    assert search.calculate_max_search_worker_processes() == 1

def test_calculate_max_search_worker_processes_lt_1(monkeypatch):
    # Plan:
    # - Patch config.settings["MAX_CONCURRENT_SEARCH_PROCESSES"] to 0
    # - Should return 1 (never less than 1)
    class FakeConfig:
        settings = {"MAX_CONCURRENT_SEARCH_PROCESSES": 0}
    monkeypatch.setattr("search.config", FakeConfig)
    assert search.calculate_max_search_worker_processes() == 1

def test_calculate_max_search_worker_processes_eq_2(monkeypatch):
    # Plan:
    # - Patch config.settings["MAX_CONCURRENT_SEARCH_PROCESSES"] to 2
    # - Should return 1
    class FakeConfig:
        settings = {"MAX_CONCURRENT_SEARCH_PROCESSES": 2}
    monkeypatch.setattr("search.config", FakeConfig)
    assert search.calculate_max_search_worker_processes() == 1

def test_initiate_warc_gz_read_threads_basic(monkeypatch):
    # Plan:
    # - Patch log_info to record call
    # - Patch ThreadPoolExecutor to simulate context manager and submit
    # - Patch Thread to simulate monitor thread
    # - Patch as_completed to yield fake futures
    # - Patch config.settings["MAX_RAM_USAGE_PERCENT"]
    # - Patch PAUSE_READ_THREADS_EVENT.set to record call

    called = {}

    # Patch log_info
    monkeypatch.setattr("search.log_info", lambda msg: called.setdefault("log_info", msg))

    # Patch PAUSE_READ_THREADS_EVENT.set
    monkeypatch.setattr(search.PAUSE_READ_THREADS_EVENT, "set", lambda: called.setdefault("pause_set", True))

    # Patch config.settings
    class FakeConfig:
        settings = {"MAX_RAM_USAGE_PERCENT": 42}
    monkeypatch.setattr("search.config", FakeConfig)

    # Patch ThreadPoolExecutor
    class FakeFuture:
        def result(self): called.setdefault("future_result", True)
        def done(self): return True
    class FakeExecutor:
        def __init__(self, max_workers=None): called["executor_max_workers"] = max_workers
        def __enter__(self): return self
        def __exit__(self, exc_type, exc_val, exc_tb): called["executor_exit"] = True
        def submit(self, fn, arg):
            called.setdefault("submit_calls", []).append(arg)
            return FakeFuture()
    monkeypatch.setattr("search.ThreadPoolExecutor", FakeExecutor)

    # Patch as_completed to just yield the tasks
    monkeypatch.setattr("search.as_completed", lambda tasks: list(tasks))

    # Patch Thread to simulate monitor thread
    class FakeThread:
        def __init__(self, target, args):
            called["monitor_thread_args"] = args
        def start(self): called["monitor_thread_started"] = True
        def join(self): called["monitor_thread_joined"] = True
    monkeypatch.setattr("search.Thread", FakeThread)

    # Patch read_warc_gz_records to a dummy function
    monkeypatch.setattr("search.read_warc_gz_records", lambda path: None)

    # Run
    warc_gz_files = ["a.gz", "b.gz"]
    search.initiate_warc_gz_read_threads(warc_gz_files)

    # Assert
    assert called["log_info"].startswith("Reading records from 2")
    assert called["pause_set"] is True
    assert called["executor_max_workers"] == 4
    assert set(called["submit_calls"]) == set(warc_gz_files)
    assert called["monitor_thread_args"][1] == 42
    assert called["monitor_thread_started"] is True
    assert called["future_result"] is True
    assert called["monitor_thread_joined"] is True
    assert called["executor_exit"] is True

def test_initiate_warc_gz_read_threads_empty(monkeypatch):
    # Plan:
    # - Should handle empty warc_gz_files list gracefully

    called = {}

    monkeypatch.setattr("search.log_info", lambda msg: called.setdefault("log_info", msg))
    monkeypatch.setattr(search.PAUSE_READ_THREADS_EVENT, "set", lambda: called.setdefault("pause_set", True))
    class FakeConfig:
        settings = {"MAX_RAM_USAGE_PERCENT": 99}
    monkeypatch.setattr("search.config", FakeConfig)
    class FakeExecutor:
        def __init__(self, max_workers=None): called["executor_max_workers"] = max_workers
        def __enter__(self): return self
        def __exit__(self, exc_type, exc_val, exc_tb): called["executor_exit"] = True
        def submit(self, fn, arg): raise AssertionError("Should not submit any tasks")
    monkeypatch.setattr("search.ThreadPoolExecutor", FakeExecutor)
    monkeypatch.setattr("search.as_completed", lambda tasks: list(tasks))
    class FakeThread:
        def __init__(self, target, args): called["monitor_thread_args"] = args
        def start(self): called["monitor_thread_started"] = True
        def join(self): called["monitor_thread_joined"] = True
    monkeypatch.setattr("search.Thread", FakeThread)
    monkeypatch.setattr("search.read_warc_gz_records", lambda path: None)

    search.initiate_warc_gz_read_threads([])

    assert called["log_info"].startswith("Reading records from 0")
    assert called["pause_set"] is True
    assert called["executor_max_workers"] == 4
    assert called["monitor_thread_args"][1] == 99
    assert called["monitor_thread_started"] is True
    assert called["monitor_thread_joined"] is True
    assert called["executor_exit"] is True

def test_initiate_warc_gz_read_threads_monitor_thread_receives_tasks(monkeypatch):
    # Plan:
    # - Ensure the monitor thread receives the correct set of tasks

    received_tasks = {}

    monkeypatch.setattr("search.log_info", lambda msg: None)
    monkeypatch.setattr(search.PAUSE_READ_THREADS_EVENT, "set", lambda: None)
    class FakeConfig:
        settings = {"MAX_RAM_USAGE_PERCENT": 77}
    monkeypatch.setattr("search.config", FakeConfig)
    class FakeFuture:
        def result(self): pass
        def done(self): return True
    class FakeExecutor:
        def __init__(self, max_workers=None): pass
        def __enter__(self): return self
        def __exit__(self, exc_type, exc_val, exc_tb): pass
        def submit(self, fn, arg):
            f = FakeFuture()
            received_tasks[arg] = f
            return f
    monkeypatch.setattr("search.ThreadPoolExecutor", FakeExecutor)
    monkeypatch.setattr("search.as_completed", lambda tasks: list(tasks))
    def fake_thread(target, args):
        # args[0] is the set of tasks
        assert set(args[0]) == set(received_tasks.values())
        return type("T", (), {"start": lambda self: None, "join": lambda self: None})()
    monkeypatch.setattr("search.Thread", fake_thread)
    monkeypatch.setattr("search.read_warc_gz_records", lambda path: None)

    files = ["f1.gz", "f2.gz", "f3.gz"]
    search.initiate_warc_gz_read_threads(files)

def test_monitoring_thread_basic(monkeypatch, capsys):
    # Simulate two tasks, done after 3 iterations
    class FakeFuture:
        def __init__(self):
            self.calls = 0
        def done(self):
            self.calls += 1
            # Not done for first 2 calls, done on 3rd
            return self.calls >= 3

    fake_futures = {FakeFuture(), FakeFuture()}

    # Patch SEARCH_QUEUE.qsize and TOTAL_RECORDS_READ
    monkeypatch.setattr("search.SEARCH_QUEUE", type("Q", (), {"qsize": staticmethod(lambda: 42)})())
    search.TOTAL_RECORDS_READ = 123

    # Patch get_total_ram_used_percent to return a fixed value
    monkeypatch.setattr("search.get_total_ram_used_percent", lambda: 55)

    # Record calls to monitor_ram_usage
    called = {}
    def fake_monitor_ram_usage(ram, target):
        called.setdefault("monitor_calls", []).append((ram, target))
    monkeypatch.setattr("search.monitor_ram_usage", fake_monitor_ram_usage)

    # Patch time.sleep to avoid delay
    monkeypatch.setattr(search.time, "sleep", lambda s: called.setdefault("slept", True))

    # Run
    search.monitoring_thread(fake_futures, 77)

    # Check monitor_ram_usage called with correct values
    assert all(ram == 55 and target == 77 for ram, target in called["monitor_calls"])
    # Should have slept at least once
    assert called["slept"] is True
    # Check print output
    out = capsys.readouterr().out
    assert "Total WARC records read: 123" in out
    assert "Records in the search queue: 42" in out
    assert "RAM used: 55%" in out

def test_monitoring_thread_tasks_already_done(monkeypatch, capsys):
    # All tasks done immediately
    class DoneFuture:
        def done(self): return True
    fake_futures = {DoneFuture(), DoneFuture()}

    monkeypatch.setattr("search.SEARCH_QUEUE", type("Q", (), {"qsize": staticmethod(lambda: 0)})())
    search.TOTAL_RECORDS_READ = 0
    monkeypatch.setattr("search.get_total_ram_used_percent", lambda: 1)
    monkeypatch.setattr("search.monitor_ram_usage", lambda ram, target: None)
    monkeypatch.setattr(search.time, "sleep", lambda s: None)

    # Should not print anything or call monitor_ram_usage
    search.monitoring_thread(fake_futures, 10)
    out = capsys.readouterr().out
    assert out == ""

def test_monitoring_thread_multiple_iterations(monkeypatch, capsys):
    # Simulate 2 tasks, one finishes after 2, one after 4
    class FakeFuture:
        def __init__(self, done_after):
            self.calls = 0
            self.done_after = done_after
        def done(self):
            self.calls += 1
            return self.calls > self.done_after

    f1 = FakeFuture(2)
    f2 = FakeFuture(4)
    fake_futures = {f1, f2}

    monkeypatch.setattr("search.SEARCH_QUEUE", type("Q", (), {"qsize": staticmethod(lambda: 7)})())
    search.TOTAL_RECORDS_READ = 99
    monkeypatch.setattr("search.get_total_ram_used_percent", lambda: 33)
    monitor_calls = []
    monkeypatch.setattr("search.monitor_ram_usage", lambda ram, target: monitor_calls.append((ram, target)))
    monkeypatch.setattr(search.time, "sleep", lambda s: None)

    search.monitoring_thread(fake_futures, 88)
    # Should have called monitor_ram_usage at least as many times as the max done_after
    assert len(monitor_calls) >= 4
    out = capsys.readouterr().out
    assert "Total WARC records read: 99" in out
    assert "Records in the search queue: 7" in out
    assert "RAM used: 33%" in out

def test_monitor_ram_usage_exceeds_limit(monkeypatch):
    # Plan:
    # - Patch log_warning to record call
    # - Patch PAUSE_READ_THREADS_EVENT.clear to record call
    # - Patch PAUSE_READ_THREADS_EVENT.is_set to return True (simulate already set)
    # - Patch time.sleep to avoid delay
    # - Call monitor_ram_usage with ram_in_use_percent >= max_ram_usage_percent_target
    # - Assert log_warning and clear called, set not called

    called = {}

    monkeypatch.setattr("search.log_warning", lambda msg: called.setdefault("log_warning", msg))
    monkeypatch.setattr(search.PAUSE_READ_THREADS_EVENT, "clear", lambda: called.setdefault("clear", True))
    monkeypatch.setattr(search.PAUSE_READ_THREADS_EVENT, "is_set", lambda: True)
    monkeypatch.setattr(search.PAUSE_READ_THREADS_EVENT, "set", lambda: called.setdefault("set", True))
    monkeypatch.setattr(search.time, "sleep", lambda s: called.setdefault("slept", s))

    search.monitor_ram_usage(90, 80)
    assert "log_warning" in called
    assert called["clear"] is True
    assert called["slept"] == 10
    assert "set" not in called  # set should not be called in this branch

def test_monitor_ram_usage_below_limit_event_not_set(monkeypatch):
    # Plan:
    # - Patch PAUSE_READ_THREADS_EVENT.is_set to return False (simulate not set)
    # - Patch PAUSE_READ_THREADS_EVENT.set to record call
    # - Call monitor_ram_usage with ram_in_use_percent < max_ram_usage_percent_target
    # - Assert set called

    called = {}

    monkeypatch.setattr(search.PAUSE_READ_THREADS_EVENT, "is_set", lambda: False)
    monkeypatch.setattr(search.PAUSE_READ_THREADS_EVENT, "set", lambda: called.setdefault("set", True))
    monkeypatch.setattr(search.PAUSE_READ_THREADS_EVENT, "clear", lambda: called.setdefault("clear", True))
    monkeypatch.setattr("search.log_warning", lambda msg: called.setdefault("log_warning", msg))
    monkeypatch.setattr(search.time, "sleep", lambda s: called.setdefault("slept", s))

    search.monitor_ram_usage(50, 80)
    assert called["set"] is True
    assert "clear" not in called
    assert "log_warning" not in called
    assert "slept" not in called

def test_monitor_ram_usage_below_limit_event_already_set(monkeypatch):
    # Plan:
    # - Patch PAUSE_READ_THREADS_EVENT.is_set to return True (already set)
    # - Patch PAUSE_READ_THREADS_EVENT.set to record call (should not be called)
    # - Call monitor_ram_usage with ram_in_use_percent < max_ram_usage_percent_target
    # - Assert set not called

    called = {}

    monkeypatch.setattr(search.PAUSE_READ_THREADS_EVENT, "is_set", lambda: True)
    monkeypatch.setattr(search.PAUSE_READ_THREADS_EVENT, "set", lambda: called.setdefault("set", True))
    monkeypatch.setattr(search.PAUSE_READ_THREADS_EVENT, "clear", lambda: called.setdefault("clear", True))
    monkeypatch.setattr("search.log_warning", lambda msg: called.setdefault("log_warning", msg))
    monkeypatch.setattr(search.time, "sleep", lambda s: called.setdefault("slept", s))

    search.monitor_ram_usage(50, 80)
    assert "set" not in called
    assert "clear" not in called
    assert "log_warning" not in called
    assert "slept" not in called

def test_monitor_ram_usage_exactly_at_limit(monkeypatch):
    # Plan:
    # - Patch log_warning, clear, sleep
    # - Call with ram_in_use_percent == max_ram_usage_percent_target
    # - Should trigger warning, clear, sleep

    called = {}

    monkeypatch.setattr("search.log_warning", lambda msg: called.setdefault("log_warning", msg))
    monkeypatch.setattr(search.PAUSE_READ_THREADS_EVENT, "clear", lambda: called.setdefault("clear", True))
    monkeypatch.setattr(search.PAUSE_READ_THREADS_EVENT, "is_set", lambda: True)
    monkeypatch.setattr(search.PAUSE_READ_THREADS_EVENT, "set", lambda: called.setdefault("set", True))
    monkeypatch.setattr(search.time, "sleep", lambda s: called.setdefault("slept", s))

    search.monitor_ram_usage(80, 80)
    assert "log_warning" in called
    assert called["clear"] is True
    assert called["slept"] == 10
    assert "set" not in called

def test_read_warc_gz_records_puts_records(monkeypatch):
    # Plan:
    # - Patch FileStream, GZipStream, ArchiveIterator to simulate one record
    # - Patch PAUSE_READ_THREADS_EVENT.wait to do nothing
    # - Patch SEARCH_QUEUE.put to record calls
    # - Patch WarcRecord to a simple namedtuple
    # - Patch log_warning, log_error to record calls
    # - Patch os.path.basename to return a fixed name

    called = {}

    class DummyFileStream:
        def __init__(self, path, mode): called["file_stream"] = path
        def __enter__(self): return self
        def __exit__(self, exc_type, exc_val, exc_tb): called["file_stream_exit"] = True

    class DummyGZipStream:
        def __init__(self, file_stream): called["gz_stream"] = True
        def __enter__(self): return self
        def __exit__(self, exc_type, exc_val, exc_tb): called["gz_stream_exit"] = True

    class DummyRecord:
        headers = {'WARC-Target-URI': 'http://example.com'}
        class reader:
            @staticmethod
            def read():
                return b"content"

    class DummyArchiveIterator:
        def __init__(self, *a, **k): pass
        def __iter__(self): return iter([DummyRecord()])
        def __bool__(self): return True

    monkeypatch.setattr("search.FileStream", DummyFileStream)
    monkeypatch.setattr("search.GZipStream", DummyGZipStream)
    monkeypatch.setattr("search.ArchiveIterator", lambda *a, **k: DummyArchiveIterator())
    monkeypatch.setattr(search.PAUSE_READ_THREADS_EVENT, "wait", lambda: None)
    class DummyQueue:
        def __init__(self): self.items = []
        def put(self, item): self.items.append(item)
    dummy_queue = DummyQueue()
    monkeypatch.setattr("search.SEARCH_QUEUE", dummy_queue)
    # Patch WarcRecord to just store args
    monkeypatch.setattr("search.WarcRecord", lambda parent_warc_gz_file, name, contents: ("WARC", parent_warc_gz_file, name, contents))
    monkeypatch.setattr("search.log_warning", lambda msg: called.setdefault("log_warning", msg))
    monkeypatch.setattr("search.log_error", lambda msg: called.setdefault("log_error", msg))
    monkeypatch.setattr("search.os.path.basename", lambda path: "file.gz")

    # Run
    search.read_warc_gz_records("somefile.gz")

    # Assert
    assert dummy_queue.items
    assert dummy_queue.items[0][1] == "somefile.gz"
    assert dummy_queue.items[0][2] == "http://example.com"
    assert dummy_queue.items[0][3] == b"content"
    assert "log_warning" not in called
    assert "log_error" not in called

def test_read_warc_gz_records_no_records(monkeypatch):
    # Plan:
    # - Patch ArchiveIterator to return empty iterator
    # - Patch log_warning to record call
    # - Patch os.path.basename

    called = {}

    class DummyFileStream:
        def __init__(self, path, mode): pass
        def __enter__(self): return self
        def __exit__(self, exc_type, exc_val, exc_tb): pass

    class DummyGZipStream:
        def __init__(self, file_stream): pass
        def __enter__(self): return self
        def __exit__(self, exc_type, exc_val, exc_tb): pass

    class DummyArchiveIterator:
        def __init__(self, *a, **k): pass
        def __iter__(self): return iter([])
        def __bool__(self): return False

    monkeypatch.setattr("search.FileStream", DummyFileStream)
    monkeypatch.setattr("search.GZipStream", DummyGZipStream)
    monkeypatch.setattr("search.ArchiveIterator", lambda *a, **k: DummyArchiveIterator())
    monkeypatch.setattr(search.PAUSE_READ_THREADS_EVENT, "wait", lambda: None)
    class DummyQueue:
        def put(self, item): raise AssertionError("Should not put any items")
    monkeypatch.setattr("search.SEARCH_QUEUE", DummyQueue())
    monkeypatch.setattr("search.log_warning", lambda msg: called.setdefault("log_warning", msg))
    monkeypatch.setattr("search.log_error", lambda msg: called.setdefault("log_error", msg))
    monkeypatch.setattr("search.os.path.basename", lambda path: "file.gz")

    search.read_warc_gz_records("emptyfile.gz")
    assert "log_warning" in called
    assert "No WARC records found" in called["log_warning"]
    assert "log_error" not in called

def test_read_warc_gz_records_exception(monkeypatch):
    # Plan:
    # - Patch ArchiveIterator to raise exception
    # - Patch log_error to record call
    # - Patch os.path.basename

    called = {}

    class DummyFileStream:
        def __init__(self, path, mode): pass
        def __enter__(self): return self
        def __exit__(self, exc_type, exc_val, exc_tb): pass

    class DummyGZipStream:
        def __init__(self, file_stream): pass
        def __enter__(self): return self
        def __exit__(self, exc_type, exc_val, exc_tb): pass

    def raise_exc(*a, **k):
        raise RuntimeError("fail!")

    monkeypatch.setattr("search.FileStream", DummyFileStream)
    monkeypatch.setattr("search.GZipStream", DummyGZipStream)
    monkeypatch.setattr("search.ArchiveIterator", raise_exc)
    monkeypatch.setattr(search.PAUSE_READ_THREADS_EVENT, "wait", lambda: None)
    class DummyQueue:
        def put(self, item): raise AssertionError("Should not put any items")
    monkeypatch.setattr("search.SEARCH_QUEUE", DummyQueue())
    monkeypatch.setattr("search.log_warning", lambda msg: called.setdefault("log_warning", msg))
    monkeypatch.setattr("search.log_error", lambda msg: called.setdefault("log_error", msg))
    monkeypatch.setattr("search.os.path.basename", lambda path: "file.gz")

    search.read_warc_gz_records("errfile.gz")
    assert "log_error" in called
    assert "Error ocurred when reading file.gz" in called["log_error"]

def test_search_worker_process_processes_records(monkeypatch):
    # Plan:
    # - Provide a fake queue with two WarcRecord objects and a final None to stop
    # - Patch initialize_worker_process_resources to provide fake buffers and zips
    # - Patch search_warc_record to record calls
    # - Patch finalize_worker_process_resources to record call

    class FakeQueue:
        def __init__(self, items):
            self.items = items
        def get(self):
            return self.items.pop(0)

    # Prepare two fake records and a None to signal stop
    record1 = object()
    record2 = object()
    fake_queue = FakeQueue([record1, record2, None])

    called = {}

    def fake_init_worker_proc_resources(results_and_regexes_dict, zip_files_with_matches):
        called["init"] = (results_and_regexes_dict, zip_files_with_matches)
        return {"buf": "buffer"}, {"zip": "zipfile"}

    def fake_search_warc_record(warc_record, results_and_regexes_dict, result_files_write_buffers, zip_archives_dict, zip_files_with_matches):
        called.setdefault("records", []).append(warc_record)

    def fake_finalize_worker_proc_resources(results_and_regexes_dict, results_files_locks_dict, result_files_write_buffers, zip_archives_dict):
        called["finalize"] = (results_and_regexes_dict, results_files_locks_dict, result_files_write_buffers, zip_archives_dict)

    monkeypatch.setattr("search.initialize_worker_process_resources", fake_init_worker_proc_resources)
    monkeypatch.setattr("search.search_warc_record", fake_search_warc_record)
    monkeypatch.setattr("search.finalize_worker_process_resources", fake_finalize_worker_proc_resources)

    # Dummy dicts for arguments
    results_and_regexes_dict = {"f.txt": "regex"}
    results_files_locks_dict = {"f.txt": object()}
    zip_files_with_matches = True

    # Run
    search.search_worker_process(fake_queue, results_and_regexes_dict, results_files_locks_dict, zip_files_with_matches)

    # Assert
    assert called["init"] == (results_and_regexes_dict, zip_files_with_matches)
    assert called["records"] == [record1, record2]
    assert called["finalize"][0] == results_and_regexes_dict
    assert called["finalize"][1] == results_files_locks_dict

def test_search_worker_process_stops_on_none(monkeypatch):
    # Plan:
    # - Provide a fake queue that returns None immediately
    # - Patch initialize_worker_process_resources and finalize_worker_process_resources to record calls
    # - search_warc_record should not be called

    class FakeQueue:
        def get(self):
            return None

    called = {}

    def fake_init_worker_proc_resources(results_and_regexes_dict, zip_files_with_matches):
        called["init"] = True
        return {}, {}

    def fake_finalize_worker_proc_resources(results_and_regexes_dict, results_files_locks_dict, result_files_write_buffers, zip_archives_dict):
        called["finalize"] = True

    def fake_search_warc_record(*a, **k):
        called["search"] = True

    monkeypatch.setattr("search.initialize_worker_process_resources", fake_init_worker_proc_resources)
    monkeypatch.setattr("search.finalize_worker_process_resources", fake_finalize_worker_proc_resources)
    monkeypatch.setattr("search.search_warc_record", fake_search_warc_record)

    results_and_regexes_dict = {}
    results_files_locks_dict = {}
    zip_files_with_matches = False

    search.search_worker_process(FakeQueue(), results_and_regexes_dict, results_files_locks_dict, zip_files_with_matches)

    assert called["init"] is True
    assert called["finalize"] is True
    assert "search" not in called

def test_search_worker_process_multiple_nones(monkeypatch):
    # Plan:
    # - Provide a fake queue that returns some records, then several None values
    # - Only the first None should trigger finalize and break

    class FakeQueue:
        def __init__(self):
            self.items = [object(), None, None]
        def get(self):
            return self.items.pop(0)

    called = {}

    def fake_init_worker_proc_resources(results_and_regexes_dict, zip_files_with_matches):
        return {}, {}

    def fake_finalize_worker_proc_resources(results_and_regexes_dict, results_files_locks_dict, result_files_write_buffers, zip_archives_dict):
        called.setdefault("finalize_count", 0)
        called["finalize_count"] += 1

    def fake_search_warc_record(warc_record, *a, **k):
        called.setdefault("records", []).append(warc_record)

    monkeypatch.setattr("search.initialize_worker_process_resources", fake_init_worker_proc_resources)
    monkeypatch.setattr("search.finalize_worker_process_resources", fake_finalize_worker_proc_resources)
    monkeypatch.setattr("search.search_warc_record", fake_search_warc_record)

    results_and_regexes_dict = {}
    results_files_locks_dict = {}
    zip_files_with_matches = False

    search.search_worker_process(FakeQueue(), results_and_regexes_dict, results_files_locks_dict, zip_files_with_matches)

    # Only one record processed, finalize called once
    assert len(called["records"]) == 1
    assert called["finalize_count"] == 1

def test_initialize_worker_process_resources_no_zip(monkeypatch):
    # Plan:
    # - Provide a dict of result files
    # - zip_files_with_matches=False
    # - Should return a dict of StringIOs, empty zip_archives_dict, no directory created

    called = {}

    # Patch os.makedirs to fail if called
    monkeypatch.setattr("os.makedirs", lambda path: (_ for _ in ()).throw(AssertionError("Should not create dir")))
    # Patch os.getpid to a fixed value
    monkeypatch.setattr("os.getpid", lambda: 12345)

    # Provide a fake get_base_file_name (should not be called)
    monkeypatch.setattr("search.get_base_file_name", lambda path: (_ for _ in ()).throw(AssertionError("Should not call get_base_file_name")))
    # Patch zipfile.ZipFile to fail if called
    monkeypatch.setattr("zipfile.ZipFile", lambda *a, **k: (_ for _ in ()).throw(AssertionError("Should not create zip")))

    dct = {"/tmp/results1.txt": "regex1", "/tmp/results2.txt": "regex2"}
    buffers, zips = search.initialize_worker_process_resources(dct, zip_files_with_matches=False)
    assert set(buffers.keys()) == set(dct.keys())
    for v in buffers.values():
        assert isinstance(v, StringIO)
    assert zips == {}

def test_initialize_worker_process_resources_with_zip(monkeypatch, tmp_path):
    # Prepare two fake result file paths in a temporary directory.
    result_file1 = str(tmp_path / "result1.txt")
    result_file2 = str(tmp_path / "result2.txt")
    results_dict = {result_file1: "regex1", result_file2: "regex2"}
    
    # Capture the directory creation call.
    called_makedirs = []
    def fake_makedirs(path):
        called_makedirs.append(path)
    monkeypatch.setattr(search.os, "makedirs", fake_makedirs)
    
    # Monkeypatch get_base_file_name to return the file name (without extension)
    monkeypatch.setattr(search, "get_base_file_name", lambda path: os.path.splitext(os.path.basename(path))[0])
    
    # Replace ZipFile with a dummy that records its filename.
    class DummyZip:
        def __init__(self, filename, mode, compression):
            self.filename = filename
        def close(self):
            pass
    monkeypatch.setattr(search.zipfile, "ZipFile", DummyZip)
    
    # Call the function under test.
    buffers, zips = search.initialize_worker_process_resources(results_dict, zip_files_with_matches=True)
    
    # Assert the buffers dictionary has one StringIO per result file.
    assert set(buffers.keys()) == set(results_dict.keys())
    for buf in buffers.values():
        assert isinstance(buf, StringIO)
    
    # Expect a zip archive per result file.
    assert len(zips) == len(results_dict)
    
    # Compute the expected temporary directory.
    # The function uses the dirname of the first key then appends "/temp/<pid>"
    results_dir = os.path.dirname(result_file1)
    expected_zip_dir = os.path.join(f"{results_dir}/temp", str(os.getpid()))
    assert expected_zip_dir in called_makedirs
    
    # Verify that each expected result produces a zip file in the archive dict.
    for results_file in results_dict.keys():
        base_name = os.path.splitext(os.path.basename(results_file))[0]
        expected_zip_path = os.path.join(expected_zip_dir, f"{base_name}.zip")
        assert expected_zip_path in zips
        assert isinstance(zips[expected_zip_path], DummyZip)


def test_initialize_worker_process_resources_empty_dict(monkeypatch):
    # For an empty dictionary no directories or zip files should be created.
    def fail_on_call(*args, **kwargs):
        raise AssertionError("Should not call this function")
    monkeypatch.setattr(search.os, "makedirs", fail_on_call)
    monkeypatch.setattr(search.zipfile, "ZipFile", fail_on_call)
    
    buffers, zips = search.initialize_worker_process_resources({}, zip_files_with_matches=True)
    assert buffers == {}
    assert zips == {}

def test_initialize_worker_process_resources_zipfile_exception(monkeypatch, tmp_path):
    # Plan:
    # - Simulate ZipFile raising an exception
    # - Should propagate the exception

    monkeypatch.setattr("os.makedirs", lambda path: None)
    monkeypatch.setattr("os.getpid", lambda: 42)
    monkeypatch.setattr("search.get_base_file_name", lambda path: "basename")
    def raise_zip(*a, **k): raise RuntimeError("zipfail")
    monkeypatch.setattr("zipfile.ZipFile", raise_zip)

    dct = {str(tmp_path / "f.txt"): "r"}
    try:
        search.initialize_worker_process_resources(dct, zip_files_with_matches=True)
        assert False, "Should have raised"
    except RuntimeError as e:
        assert "zipfail" in str(e)

def test_search_warc_record_match_in_name(monkeypatch):
    # Plan:
    # - Simulate a WARC record whose name matches the regex, but contents do not
    # - Patch find_regex_matches, is_file_binary, write_record_info_to_result_output_buffer
    # - Ensure write_record_info_to_result_output_buffer is called with correct args
    called = {}

    class DummyRecord:
        parent_warc_gz_file = "parent.gz"
        name = "http://example.com"
        contents = b"not matching content"

    warc_record = DummyRecord()
    results_and_regexes_dict = {"result.txt": "regex"}
    result_files_write_buffers = {"result.txt": "buffer"}
    zip_archives_dict = {}
    zip_files_with_matches = False

    def fake_find_regex_matches(val, regex):
        if val == warc_record.name:
            return ["match"]
        return []
    monkeypatch.setattr("search.find_regex_matches", fake_find_regex_matches)
    monkeypatch.setattr("search.is_file_binary", lambda contents: False)
    def fake_write_record_info_to_result_output_buffer(buf, matches_in_name, matches_in_contents, parent, name):
        called["write"] = (buf, matches_in_name, matches_in_contents, parent, name)
    monkeypatch.setattr("search.write_record_info_to_result_output_buffer", fake_write_record_info_to_result_output_buffer)

    search.search_warc_record(
        warc_record,
        results_and_regexes_dict,
        result_files_write_buffers,
        zip_archives_dict,
        zip_files_with_matches
    )
    assert called["write"][0] == "buffer"
    assert called["write"][1] == ["match"]
    assert called["write"][2] == []
    assert called["write"][3] == "parent.gz"
    assert called["write"][4] == "http://example.com"

def test_search_warc_record_match_in_contents(monkeypatch):
    # Plan:
    # - Simulate a WARC record whose contents match the regex, but name does not
    # - Patch find_regex_matches, is_file_binary, write_record_info_to_result_output_buffer
    # - Ensure write_record_info_to_result_output_buffer is called with correct args
    called = {}

    class DummyRecord:
        parent_warc_gz_file = "parent.gz"
        name = "no-match"
        contents = b"some matching content"

    warc_record = DummyRecord()
    results_and_regexes_dict = {"result.txt": "regex"}
    result_files_write_buffers = {"result.txt": "buffer"}
    zip_archives_dict = {}
    zip_files_with_matches = False

    def fake_find_regex_matches(val, regex):
        if isinstance(val, str) and "matching" in val:
            return ["found"]
        return []
    monkeypatch.setattr("search.find_regex_matches", fake_find_regex_matches)
    monkeypatch.setattr("search.is_file_binary", lambda contents: False)
    def fake_write_record_info_to_result_output_buffer(buf, matches_in_name, matches_in_contents, parent, name):
        called["write"] = (buf, matches_in_name, matches_in_contents, parent, name)
    monkeypatch.setattr("search.write_record_info_to_result_output_buffer", fake_write_record_info_to_result_output_buffer)

    search.search_warc_record(
        warc_record,
        results_and_regexes_dict,
        result_files_write_buffers,
        zip_archives_dict,
        zip_files_with_matches
    )
    assert called["write"][0] == "buffer"
    assert called["write"][1] == []
    assert called["write"][2] == ["found"]
    assert called["write"][3] == "parent.gz"
    assert called["write"][4] == "no-match"

def test_search_warc_record_binary_file_skipped(monkeypatch):
    # Plan:
    # - Simulate a binary file, SEARCH_BINARY_FILES is False
    # - Ensure matches_in_contents is '', and write_record_info_to_result_output_buffer is called if matches_in_name
    called = {}

    class DummyConfig:
        settings = {"SEARCH_BINARY_FILES": False}
    monkeypatch.setattr("search.config", DummyConfig)

    class DummyRecord:
        parent_warc_gz_file = "parent.gz"
        name = "bin"
        contents = b"\x00\x01"

    warc_record = DummyRecord()
    results_and_regexes_dict = {"result.txt": "regex"}
    result_files_write_buffers = {"result.txt": "buffer"}
    zip_archives_dict = {}
    zip_files_with_matches = False

    monkeypatch.setattr("search.find_regex_matches", lambda val, regex: ["nm"] if val == "bin" else [])
    monkeypatch.setattr("search.is_file_binary", lambda contents: True)
    def fake_write_record_info_to_result_output_buffer(buf, matches_in_name, matches_in_contents, parent, name):
        called["write"] = (buf, matches_in_name, matches_in_contents, parent, name)
    monkeypatch.setattr("search.write_record_info_to_result_output_buffer", fake_write_record_info_to_result_output_buffer)

    search.search_warc_record(
        warc_record,
        results_and_regexes_dict,
        result_files_write_buffers,
        zip_archives_dict,
        zip_files_with_matches
    )
    assert called["write"][2] == ''

def test_search_warc_record_no_match(monkeypatch):
    # Plan:
    # - Simulate no matches in name or contents
    # - Ensure write_record_info_to_result_output_buffer is not called
    called = {}

    class DummyRecord:
        parent_warc_gz_file = "parent.gz"
        name = "nope"
        contents = b"nope"

    warc_record = DummyRecord()
    results_and_regexes_dict = {"result.txt": "regex"}
    result_files_write_buffers = {"result.txt": "buffer"}
    zip_archives_dict = {}
    zip_files_with_matches = False

    monkeypatch.setattr("search.find_regex_matches", lambda val, regex: [])
    monkeypatch.setattr("search.is_file_binary", lambda contents: False)
    def fake_write_record_info_to_result_output_buffer(*a, **k):
        called["write"] = True
    monkeypatch.setattr("search.write_record_info_to_result_output_buffer", fake_write_record_info_to_result_output_buffer)

    search.search_warc_record(
        warc_record,
        results_and_regexes_dict,
        result_files_write_buffers,
        zip_archives_dict,
        zip_files_with_matches
    )
    assert "write" not in called

def test_search_warc_record_zipfile_success(monkeypatch):
    # Plan:
    # - Simulate a match and ZIP_FILES_WITH_MATCHES True
    # - Patch get_results_zip_archive_file_path, add_file_to_zip_archive
    # - Ensure add_file_to_zip_archive is called with correct args
    called = {}

    class DummyRecord:
        parent_warc_gz_file = "parent.gz"
        name = "zipme"
        contents = b"zipcontent"

    warc_record = DummyRecord()
    results_and_regexes_dict = {"result.txt": "regex"}
    result_files_write_buffers = {"result.txt": "buffer"}
    zip_archives_dict = {"zipfile.zip": "zipobj"}
    zip_files_with_matches = True

    monkeypatch.setattr("search.find_regex_matches", lambda val, regex: ["match"])
    monkeypatch.setattr("search.is_file_binary", lambda contents: False)
    monkeypatch.setattr("search.write_record_info_to_result_output_buffer", lambda *a, **k: None)
    monkeypatch.setattr("search.get_results_zip_archive_file_path", lambda zdict, rfp: "zipfile.zip")
    def fake_add_file_to_zip_archive(name, contents, zipobj):
        called["add"] = (name, contents, zipobj)
    monkeypatch.setattr("search.add_file_to_zip_archive", fake_add_file_to_zip_archive)
    monkeypatch.setattr("search.log_error", lambda msg: called.setdefault("log_error", msg))

    search.search_warc_record(
        warc_record,
        results_and_regexes_dict,
        result_files_write_buffers,
        zip_archives_dict,
        zip_files_with_matches
    )
    assert called["add"][0] == "zipme"
    assert called["add"][1] == b"zipcontent"
    assert called["add"][2] == "zipobj"
    assert "log_error" not in called

def test_search_warc_record_zipfile_exception(monkeypatch):
    # Plan:
    # - Simulate add_file_to_zip_archive raising an exception
    # - Ensure log_error is called and function continues
    called = {}

    class DummyRecord:
        parent_warc_gz_file = "parent.gz"
        name = "zipme"
        contents = b"zipcontent"

    warc_record = DummyRecord()
    results_and_regexes_dict = {"result.txt": "regex"}
    result_files_write_buffers = {"result.txt": "buffer"}
    zip_archives_dict = {"zipfile.zip": "zipobj"}
    zip_files_with_matches = True

    monkeypatch.setattr("search.find_regex_matches", lambda val, regex: ["match"])
    monkeypatch.setattr("search.is_file_binary", lambda contents: False)
    monkeypatch.setattr("search.write_record_info_to_result_output_buffer", lambda *a, **k: None)
    monkeypatch.setattr("search.get_results_zip_archive_file_path", lambda zdict, rfp: "zipfile.zip")
    def fake_add_file_to_zip_archive(name, contents, zipobj):
        raise Exception("fail!")
    monkeypatch.setattr("search.add_file_to_zip_archive", fake_add_file_to_zip_archive)
    def fake_log_error(msg):
        called["log_error"] = msg
    monkeypatch.setattr("search.log_error", fake_log_error)

    search.search_warc_record(
        warc_record,
        results_and_regexes_dict,
        result_files_write_buffers,
        zip_archives_dict,
        zip_files_with_matches
    )
    assert "fail!" in called["log_error"]


class FakeLock:
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

class FakeZip:
    def __init__(self):
        self.closed = False
    def close(self):
        self.closed = True

def test_finalize_worker_process_resources_writes_to_file(tmp_path):
    # Prepare a temporary file to act as our results file.
    output_file = tmp_path / "result.txt"
    # Ensure the file exists.
    output_file.write_text("")
    
    # Create a StringIO buffer with some test content.
    buffer = StringIO("Test output content")
    
    # Set up the dictionaries.
    results_and_regexes_dict = {str(output_file): "dummy_regex"}
    result_files_write_buffers = {str(output_file): buffer}
    result_files_write_locks_dict = {str(output_file): FakeLock()}
    zip_archives_dict = {}  # No zip archives in this test

    # Call the function to finalize worker process resources.
    search.finalize_worker_process_resources(results_and_regexes_dict,
                                                result_files_write_locks_dict,
                                                result_files_write_buffers,
                                                zip_archives_dict)
    
    # Verify the file now contains the content from the buffer.
    file_content = output_file.read_text()
    assert "Test output content" in file_content

def test_finalize_worker_process_resources_closes_zip(tmp_path):
    # Prepare a temporary file to act as our results file.
    output_file = tmp_path / "result.txt"
    output_file.write_text("")
    
    # Create a StringIO buffer (empty for this test).
    buffer = StringIO("")
    
    # Set up a fake zip archive.
    fake_zip = FakeZip()
    
    # Use the temporary file as the result file.
    results_and_regexes_dict = {str(output_file): "dummy_regex"}
    result_files_write_buffers = {str(output_file): buffer}
    result_files_write_locks_dict = {str(output_file): FakeLock()}
    # Provide a dummy zip archive in the zip_archives_dict.
    zip_archives_dict = {str(tmp_path / "dummy.zip"): fake_zip}

    # Call the function.
    search.finalize_worker_process_resources(results_and_regexes_dict,
                                                result_files_write_locks_dict,
                                                result_files_write_buffers,
                                                zip_archives_dict)
    # Verify that the fake zip archive was closed.
    assert fake_zip.closed is True
