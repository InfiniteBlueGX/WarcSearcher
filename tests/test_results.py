import os
import re
import shutil
import sys
import pytest

import results
from multiprocessing import Manager
from io import StringIO
from results import get_results_file_path


class DummyLogger:
    def __init__(self):
        self.errors = []
    def log_error(self, msg):
        self.errors.append(msg)

@pytest.fixture(autouse=True)
def patch_dependencies(monkeypatch, tmp_path):
    # Patch config.settings
    search_dir = tmp_path / "definitions"
    search_dir.mkdir()
    monkeypatch.setattr(results.config, "settings", {
        "SEARCH_REGEX_DEFINITIONS_DIRECTORY": str(search_dir),
        "RESULTS_OUTPUT_DIRECTORY": str(tmp_path),
        "ZIP_FILES_WITH_MATCHES": False,
    })
    # Patch results_output_subdirectory global
    monkeypatch.setattr(results, "results_output_subdirectory", str(tmp_path))
    # Patch log_error to capture errors
    dummy_logger = DummyLogger()
    monkeypatch.setattr(results, "log_error", dummy_logger.log_error)
    # Patch sys.exit to raise SystemExit
    monkeypatch.setattr(sys, "exit", lambda *a, **k: (_ for _ in ()).throw(SystemExit))
    yield search_dir, dummy_logger

def write_definition_file(path, content):
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

def test_returns_dict_with_valid_regex(tmp_path, patch_dependencies):
    search_dir, _ = patch_dependencies
    def_file = search_dir / "test1.txt"
    write_definition_file(def_file, r"\d{3}-\d{2}-\d{4}")
    # Patch get_results_file_path to a predictable path
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(results, "get_results_file_path", lambda p: str(tmp_path / "test1_results.txt"))
    result = results.create_result_files_associated_with_regexes_dict()
    monkeypatch.undo()
    assert len(result) == 1
    key = str(tmp_path / "test1_results.txt")
    assert key in result
    assert isinstance(result[key], re.Pattern)
    assert result[key].pattern == r"\d{3}-\d{2}-\d{4}"

def test_skips_invalid_regex(tmp_path, patch_dependencies):
    search_dir, dummy_logger = patch_dependencies
    def_file = search_dir / "bad.txt"
    write_definition_file(def_file, r"[unclosed")
    # Patch get_results_file_path to a predictable path
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(results, "get_results_file_path", lambda p: str(tmp_path / "bad_results.txt"))
    with pytest.raises(SystemExit):
        results.create_result_files_associated_with_regexes_dict()
    assert dummy_logger.errors
    assert "Invalid regular expression" in dummy_logger.errors[0] or "No valid regex patterns" in dummy_logger.errors[-1]

def test_mixed_valid_and_invalid_regex(tmp_path, patch_dependencies):
    search_dir, dummy_logger = patch_dependencies
    good_file = search_dir / "good.txt"
    bad_file = search_dir / "bad.txt"
    write_definition_file(good_file, r"foo.*bar")
    write_definition_file(bad_file, r"(")
    # Patch get_results_file_path to a predictable path
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(results, "get_results_file_path", lambda p: str(tmp_path / (os.path.splitext(os.path.basename(p))[0] + "_results.txt")))
    result = results.create_result_files_associated_with_regexes_dict()
    monkeypatch.undo()
    assert len(result) == 1
    key = str(tmp_path / "good_results.txt")
    assert key in result
    assert isinstance(result[key], re.Pattern)
    assert result[key].pattern == r"foo.*bar"
    assert dummy_logger.errors  # Should log error for bad.txt

def test_no_definition_files(tmp_path, patch_dependencies):
    _, dummy_logger = patch_dependencies
    with pytest.raises(SystemExit):
        results.create_result_files_associated_with_regexes_dict()
    assert dummy_logger.errors
    assert "No valid regex patterns" in dummy_logger.errors[-1]

def test_get_definition_txt_files_list_finds_txt_files(tmp_path, monkeypatch):
    search_dir = tmp_path / "definitions"
    file1 = search_dir / "a.txt"
    file2 = search_dir / "b.txt"
    file3 = search_dir / "not_a_txt.md"
    file1.write_text("foo")
    file2.write_text("bar")
    file3.write_text("baz")
    monkeypatch.setitem(results.config.settings, "SEARCH_REGEX_DEFINITIONS_DIRECTORY", str(search_dir))

    found_files = results.get_definition_txt_files_list()

    assert set(found_files) == {str(file1), str(file2)}
    assert str(file3) not in found_files

def test_get_definition_txt_files_list_empty_dir(tmp_path, monkeypatch):
    search_dir = tmp_path / "definitions"
    monkeypatch.setitem(results.config.settings, "SEARCH_REGEX_DEFINITIONS_DIRECTORY", str(search_dir))
    found_files = results.get_definition_txt_files_list()
    assert found_files == []

def test_get_definition_txt_files_list_nonexistent_dir(tmp_path, monkeypatch):
    search_dir = tmp_path / "does_not_exist"
    monkeypatch.setitem(results.config.settings, "SEARCH_REGEX_DEFINITIONS_DIRECTORY", str(search_dir))
    found_files = results.get_definition_txt_files_list()
    assert found_files == []

def test_compile_regex_pattern_from_definition_file_valid(tmp_path, monkeypatch):

    file_path = tmp_path / "valid.txt"
    pattern = r"\w+@\w+\.\w+"
    file_path.write_text(pattern, encoding="utf-8")
    called = {}
    monkeypatch.setattr(results, "log_error", lambda msg: called.setdefault("log_error", msg))

    regex, success = results.compile_regex_pattern_from_definition_file(str(file_path))

    assert success is True
    assert isinstance(regex, re.Pattern)
    assert regex.pattern == pattern
    assert "log_error" not in called

def test_compile_regex_pattern_from_definition_file_invalid_regex(tmp_path, monkeypatch):
    file_path = tmp_path / "invalid.txt"
    file_path.write_text(r"[unclosed", encoding="utf-8")
    errors = []
    monkeypatch.setattr(results, "log_error", lambda msg: errors.append(msg))

    regex, success = results.compile_regex_pattern_from_definition_file(str(file_path))

    assert regex is None
    assert success is False
    assert errors
    assert "Invalid regular expression" in errors[0]

def test_compile_regex_pattern_from_definition_file_ioerror(tmp_path, monkeypatch):
    file_path = tmp_path / "doesnotexist.txt"
    errors = []
    monkeypatch.setattr(results, "log_error", lambda msg: errors.append(msg))

    regex, success = results.compile_regex_pattern_from_definition_file(str(file_path))

    assert regex is None
    assert success is False
    assert errors
    assert "Error reading file" in errors[0]

def test_compile_regex_pattern_from_definition_file_empty_file(tmp_path, monkeypatch):
    file_path = tmp_path / "empty.txt"
    file_path.write_text("", encoding="utf-8")
    errors = []
    monkeypatch.setattr(results, "log_error", lambda msg: errors.append(msg))

    regex, success = results.compile_regex_pattern_from_definition_file(str(file_path))

    # Empty string is a valid regex
    assert success is True
    assert isinstance(regex, re.Pattern)
    assert regex.pattern == ""
    assert not errors

def test_initialize_results_output_subdirectory_creates_directory(tmp_path, monkeypatch):
    created_dirs = []
    log_messages = []
    monkeypatch.setitem(results.config.settings, "RESULTS_OUTPUT_DIRECTORY", str(tmp_path))
    monkeypatch.setitem(results.config.settings, "ZIP_FILES_WITH_MATCHES", False)
    monkeypatch.setattr(results, "log_info", lambda msg: log_messages.append(msg))
    monkeypatch.setattr(results, "results_output_subdirectory", "")
    orig_makedirs = os.makedirs
    def fake_makedirs(path, *a, **k):
        created_dirs.append(path)
        orig_makedirs(path, exist_ok=True)
    monkeypatch.setattr(os, "makedirs", fake_makedirs)

    results.initialize_results_output_subdirectory()

    assert any("WarcSearcher_Results_" in d for d in created_dirs)
    assert results.results_output_subdirectory in created_dirs
    assert any("Results output folder created" in msg for msg in log_messages)
    assert os.path.isdir(results.results_output_subdirectory)

def test_initialize_results_output_subdirectory_creates_temp_if_zip_enabled(tmp_path, monkeypatch):
    created_dirs = []
    log_messages = []
    monkeypatch.setitem(results.config.settings, "RESULTS_OUTPUT_DIRECTORY", str(tmp_path))
    monkeypatch.setitem(results.config.settings, "ZIP_FILES_WITH_MATCHES", True)
    monkeypatch.setattr(results, "log_info", lambda msg: log_messages.append(msg))
    monkeypatch.setattr(results, "results_output_subdirectory", "")
    orig_makedirs = os.makedirs
    def fake_makedirs(path, *a, **k):
        created_dirs.append(path)
        orig_makedirs(path, exist_ok=True)
    monkeypatch.setattr(os, "makedirs", fake_makedirs)

    results.initialize_results_output_subdirectory()

    temp_dir = os.path.join(results.results_output_subdirectory, "temp")
    assert temp_dir in created_dirs
    assert os.path.isdir(temp_dir)

def test_initialize_results_output_subdirectory_sets_global(tmp_path, monkeypatch):
    monkeypatch.setitem(results.config.settings, "RESULTS_OUTPUT_DIRECTORY", str(tmp_path))
    monkeypatch.setitem(results.config.settings, "ZIP_FILES_WITH_MATCHES", False)
    monkeypatch.setattr(results, "log_info", lambda msg: None)
    monkeypatch.setattr(results, "results_output_subdirectory", "")

    results.initialize_results_output_subdirectory()

    assert results.results_output_subdirectory.startswith(str(tmp_path))
    assert os.path.isdir(results.results_output_subdirectory)

def test_create_result_files_write_locks_dict_creates_locks(tmp_path):
    # Prepare dummy file paths
    file_paths = [str(tmp_path / f"file_{i}.txt") for i in range(3)]
    with Manager() as manager:
        locks_dict = results.create_result_files_write_locks_dict(manager, file_paths)
        # Should be a dict-like object with the same keys as file_paths
        assert set(locks_dict.keys()) == set(file_paths)
        # Each value should be a LockProxy (multiprocessing.managers.AcquirerProxy)
        for lock in locks_dict.values():
            # The proxy type name is 'AcquirerProxy'
            assert "AcquirerProxy" in type(lock).__name__

def test_create_result_files_write_locks_dict_empty_list(tmp_path):
    file_paths = []
    with Manager() as manager:
        locks_dict = results.create_result_files_write_locks_dict(manager, file_paths)
        assert dict(locks_dict) == {}

def test_create_result_files_write_locks_dict_lock_is_unique(tmp_path):
    file_paths = [str(tmp_path / "a.txt"), str(tmp_path / "b.txt")]
    with Manager() as manager:
        locks_dict = results.create_result_files_write_locks_dict(manager, file_paths)
        # Locks for different files should not be the same object
        assert locks_dict[file_paths[0]] != locks_dict[file_paths[1]]
        def test_write_result_files_headers_creates_headers(tmp_path, monkeypatch):

            # Prepare dummy results file paths and regex patterns
            file1 = tmp_path / "file1_results.txt"
            file2 = tmp_path / "file2_results.txt"
            regex1 = re.compile(r"foo\d+bar")
            regex2 = re.compile(r"baz.*qux")
            results_and_regexes = {
                str(file1): regex1,
                str(file2): regex2,
            }

            # Ensure files do not exist before
            for f in [file1, file2]:
                if f.exists():
                    f.unlink()

            # Call the function
            results.write_result_files_headers(results_and_regexes)

            # Check that files are created and contain expected headers
            for file_path, regex in results_and_regexes.items():
                with open(file_path, encoding="utf-8") as f:
                    content = f.read()
                    # File name in header
                    assert f"[{os.path.basename(file_path)}]" in content
                    # Created timestamp present
                    assert "[Created: " in content
                    # Regex pattern present
                    assert "[Regex used]" in content
                    assert regex.pattern in content
                    # Separator line
                    assert "___________________________________________________________________" in content

def test_write_result_files_headers_appends_to_existing_file(tmp_path):
    file1 = tmp_path / "existing_results.txt"
    regex = re.compile(r"abc123")
    # Write some initial content
    file1.write_text("PREVIOUS CONTENT\n", encoding="utf-8")
    results_and_regexes = {str(file1): regex}
    results.write_result_files_headers(results_and_regexes)
    content = file1.read_text(encoding="utf-8")
    # Should contain both previous content and new header
    assert "PREVIOUS CONTENT" in content
    assert "[Regex used]" in content
    assert regex.pattern in content

def test_write_result_files_headers_handles_non_ascii_regex(tmp_path):
    file1 = tmp_path / "unicode_results.txt"
    regex = re.compile(r"café\d+")
    results_and_regexes = {str(file1): regex}
    results.write_result_files_headers(results_and_regexes)
    content = file1.read_text(encoding="utf-8")
    assert "café" in content

def test_write_record_info_to_result_output_buffer_basic(monkeypatch):
    # Patch the helper to just record calls
    called = []
    def fake_write_matches_to_result_output_buffer(output_buffer, matches_list, match_type):
        called.append((list(matches_list), match_type))
        output_buffer.write(f"MOCKED[{match_type}:{len(matches_list)}]\n")
    monkeypatch.setattr(results, "write_matches_to_result_output_buffer", fake_write_matches_to_result_output_buffer)

    buf = StringIO()
    matches_list_name = ["foo", "bar"]
    matches_list_contents = ["baz"]
    parent_warc_gz_file = "archive1.warc.gz"
    file_name = "file1.txt"

    results.write_record_info_to_result_output_buffer(
        buf, matches_list_name, matches_list_contents, parent_warc_gz_file, file_name
    )

    output = buf.getvalue()
    assert f"[Archive: {parent_warc_gz_file}]" in output
    assert f"[File: {file_name}]" in output
    assert "MOCKED[file name:2]" in output
    assert "MOCKED[file contents:1]" in output
    assert "___________________________________________________________________" in output
    assert called[0][1] == "file name"
    assert called[1][1] == "file contents"
    assert called[0][0] == matches_list_name
    assert called[1][0] == matches_list_contents

def test_write_record_info_to_result_output_buffer_empty_lists(monkeypatch):
    called = []
    def fake_write_matches_to_result_output_buffer(output_buffer, matches_list, match_type):
        called.append((list(matches_list), match_type))
        output_buffer.write(f"MOCKED[{match_type}:{len(matches_list)}]\n")
    monkeypatch.setattr(results, "write_matches_to_result_output_buffer", fake_write_matches_to_result_output_buffer)

    buf = StringIO()
    results.write_record_info_to_result_output_buffer(
        buf, [], [], "archive2.warc.gz", "file2.txt"
    )
    output = buf.getvalue()
    assert "[Archive: archive2.warc.gz]" in output
    assert "[File: file2.txt]" in output
    assert "MOCKED[file name:0]" in output
    assert "MOCKED[file contents:0]" in output
    assert "___________________________________________________________________" in output
    assert called[0][0] == []
    assert called[1][0] == []

def test_write_record_info_to_result_output_buffer_real_helper(monkeypatch):
    # Use the real helper, but patch nothing
    buf = StringIO()
    matches_list_name = ["foo", "foo", "bar"]
    matches_list_contents = ["baz", "baz"]
    parent_warc_gz_file = "archive3.warc.gz"
    file_name = "file3.txt"

    results.write_record_info_to_result_output_buffer(
        buf, matches_list_name, matches_list_contents, parent_warc_gz_file, file_name
    )
    output = buf.getvalue()
    assert "[Archive: archive3.warc.gz]" in output
    assert "[File: file3.txt]" in output
    assert "[Matches found in file name:" in output
    assert "[Matches found in file contents:" in output
    assert "foo" in output
    assert "bar" in output
    assert "baz" in output
    assert "duplicates omitted" in output
    assert "___________________________________________________________________" in output

def test_write_matches_to_result_output_buffer_no_matches():
    buf = StringIO()
    results.write_matches_to_result_output_buffer(buf, [], "file name")
    output = buf.getvalue()
    # Should not write anything if matches_list is empty
    assert output == ""

def test_write_matches_to_result_output_buffer_single_match():
    buf = StringIO()
    results.write_matches_to_result_output_buffer(buf, ["foo"], "file contents")
    output = buf.getvalue()
    assert "[Matches found in file contents: 1 (0 duplicates omitted)]" in output
    assert '[Match #1 in file contents]' in output
    assert '"foo"' in output

def test_write_matches_to_result_output_buffer_multiple_unique_matches():
    buf = StringIO()
    matches = ["foo", "bar", "baz"]
    results.write_matches_to_result_output_buffer(buf, matches, "file name")
    output = buf.getvalue()
    assert "[Matches found in file name: 3 (0 duplicates omitted)]" in output
    # All unique matches should be present
    for match in matches:
        assert f'"{match}"' in output
    # Should have three match headers
    assert output.count("[Match #") == 3

def test_write_matches_to_result_output_buffer_with_duplicates():
    buf = StringIO()
    matches = ["foo", "bar", "foo", "baz", "bar"]
    results.write_matches_to_result_output_buffer(buf, matches, "file contents")
    output = buf.getvalue()
    # There are 5 total, but only 3 unique
    assert "[Matches found in file contents: 5 (2 duplicates omitted)]" in output
    # All unique matches should be present
    for match in set(matches):
        assert f'"{match}"' in output
    # Should have three match headers
    assert output.count("[Match #") == 3

def test_write_matches_to_result_output_buffer_preserves_set_uniqueness_not_order():
    buf = StringIO()
    matches = ["a", "b", "a", "c", "b"]
    results.write_matches_to_result_output_buffer(buf, matches, "file name")
    output = buf.getvalue()
    # Order is not guaranteed, but all unique should be present
    for match in set(matches):
        assert f'"{match}"' in output
    assert output.count("[Match #") == 3

def test_write_matches_to_result_output_buffer_match_type_label():
    buf = StringIO()
    matches = ["foo"]
    results.write_matches_to_result_output_buffer(buf, matches, "custom type")
    output = buf.getvalue()
    assert "[Matches found in custom type:" in output
    assert "[Match #1 in custom type]" in output

def test_move_log_file_to_results_subdirectory_moves_file(tmp_path, monkeypatch):
    # Setup: create dummy log file and results directory
    log_file = tmp_path / "log.log"
    log_file.write_text("log content", encoding="utf-8")
    results_dir = tmp_path / "results_subdir"
    results_dir.mkdir()
    # Patch results_output_subdirectory to our results_dir
    monkeypatch.setattr(results, "results_output_subdirectory", str(results_dir))
    # Patch os.getcwd to tmp_path
    monkeypatch.setattr(os, "getcwd", lambda: str(tmp_path))
    # Patch os.path.exists to return True for results_output_subdirectory
    monkeypatch.setattr(os.path, "exists", lambda p: p == str(results_dir))
    # Call function
    results.move_log_file_to_results_subdirectory()
    # Assert log file moved
    moved_log = results_dir / "log.log"
    assert moved_log.exists()
    assert moved_log.read_text(encoding="utf-8") == "log content"
    assert not log_file.exists()

def test_move_log_file_to_results_subdirectory_no_results_dir(monkeypatch, tmp_path):
    # Setup: create dummy log file
    log_file = tmp_path / "log.log"
    log_file.write_text("log content", encoding="utf-8")
    # Patch results_output_subdirectory to a non-existent dir
    non_existent_dir = tmp_path / "does_not_exist"
    monkeypatch.setattr(results, "results_output_subdirectory", str(non_existent_dir))
    # Patch os.getcwd to tmp_path
    monkeypatch.setattr(os, "getcwd", lambda: str(tmp_path))
    # Patch os.path.exists to always return False
    monkeypatch.setattr(os.path, "exists", lambda p: False)
    # Patch shutil.move to raise if called
    monkeypatch.setattr(shutil, "move", lambda src, dst: (_ for _ in ()).throw(Exception("Should not move")))
    # Should not raise
    results.move_log_file_to_results_subdirectory()
    # Log file should remain
    assert log_file.exists()
    assert log_file.read_text(encoding="utf-8") == "log content"

def test_move_log_file_to_results_subdirectory_moves_correct_file(tmp_path, monkeypatch):
    # Setup: create dummy log file in a different directory
    log_file = tmp_path / "log.log"
    log_file.write_text("log content", encoding="utf-8")
    results_dir = tmp_path / "results_subdir"
    results_dir.mkdir()
    monkeypatch.setattr(results, "results_output_subdirectory", str(results_dir))
    monkeypatch.setattr(os, "getcwd", lambda: str(tmp_path))
    monkeypatch.setattr(os.path, "exists", lambda p: p == str(results_dir))
    # Patch shutil.move to record src/dst
    moved = {}
    def fake_move(src, dst):
        moved["src"] = src
        moved["dst"] = dst
        shutil.copy(src, dst)
        os.remove(src)
    monkeypatch.setattr(shutil, "move", fake_move)
    results.move_log_file_to_results_subdirectory()
    assert moved["src"] == str(log_file)
    assert moved["dst"] == str(results_dir / "log.log")
    assert (results_dir / "log.log").exists()
    assert not log_file.exists()

def test_log_results_output_path_with_results_dir(monkeypatch, tmp_path):
    # Patch results_output_subdirectory to a non-empty string
    results_dir = tmp_path / "results_dir"
    monkeypatch.setattr(results, "results_output_subdirectory", str(results_dir))
    # Capture log_info calls
    logged = []
    monkeypatch.setattr(results, "log_info", lambda msg: logged.append(msg))
    results.log_results_output_path()
    assert logged
    assert f"Results output to: {results_dir}" in logged[0]

def test_log_results_output_path_without_results_dir(monkeypatch, tmp_path):
    # Patch results_output_subdirectory to empty string
    monkeypatch.setattr(results, "results_output_subdirectory", "")
    # Patch os.getcwd to return a known path
    monkeypatch.setattr(os, "getcwd", lambda: str(tmp_path))
    # Capture log_info calls
    logged = []
    monkeypatch.setattr(results, "log_info", lambda msg: logged.append(msg))
    results.log_results_output_path()
    assert logged
    assert f"No results folder was created due to an error. Log file output to: {tmp_path}" in logged[0]

def test_get_results_zip_archive_file_path_basic(tmp_path, monkeypatch):
    # Setup: create a dummy zip_archives_dict with one key (a temp file path)
    temp_dir = tmp_path / "temp"
    temp_dir.mkdir()
    dummy_zip_file = temp_dir / "dummy1.zip"
    dummy_zip_file.write_text("dummy", encoding="utf-8")
    zip_archives_dict = {str(dummy_zip_file): None}
    # The results file path to use
    results_file_path = str(tmp_path / "my_results.txt")
    # Patch get_base_file_name to return a predictable name
    monkeypatch.setattr("results.get_base_file_name", lambda p: "my_results")
    # Call the function
    zip_path = results.get_results_zip_archive_file_path(zip_archives_dict, results_file_path)
    # Should be in temp_dir and named my_results.zip
    expected = str(temp_dir / "my_results.zip")
    assert zip_path == expected

def test_get_results_zip_archive_file_path_uses_first_key(tmp_path, monkeypatch):
    # Setup: two different temp dirs, only first should be used
    temp_dir1 = tmp_path / "temp1"
    temp_dir2 = tmp_path / "temp2"
    temp_dir1.mkdir()
    temp_dir2.mkdir()
    dummy_zip_file1 = temp_dir1 / "a.zip"
    dummy_zip_file2 = temp_dir2 / "b.zip"
    dummy_zip_file1.write_text("a", encoding="utf-8")
    dummy_zip_file2.write_text("b", encoding="utf-8")
    zip_archives_dict = {str(dummy_zip_file1): None, str(dummy_zip_file2): None}
    results_file_path = str(tmp_path / "foo.txt")
    monkeypatch.setattr("results.get_base_file_name", lambda p: "foo")
    zip_path = results.get_results_zip_archive_file_path(zip_archives_dict, results_file_path)
    # Should use temp_dir1
    assert zip_path == str(temp_dir1 / "foo.zip")

def test_get_results_zip_archive_file_path_handles_empty_dict(monkeypatch):
    # Should raise StopIteration if dict is empty
    zip_archives_dict = {}
    results_file_path = "somefile.txt"
    with pytest.raises(StopIteration):
        results.get_results_zip_archive_file_path(zip_archives_dict, results_file_path)

def test_get_results_zip_archive_file_path_filename_matches_results(monkeypatch, tmp_path):
    # The zip file name should match the base name of the results file path
    temp_dir = tmp_path / "temp"
    temp_dir.mkdir()
    dummy_zip_file = temp_dir / "dummy2.zip"
    dummy_zip_file.write_text("dummy", encoding="utf-8")
    zip_archives_dict = {str(dummy_zip_file): None}
    results_file_path = str(tmp_path / "some_results_file.txt")
    monkeypatch.setattr("results.get_base_file_name", lambda p: "some_results_file")
    zip_path = results.get_results_zip_archive_file_path(zip_archives_dict, results_file_path)
    assert zip_path.endswith("some_results_file.zip")
    assert str(temp_dir) in zip_path

def test_finalize_results_zip_archives_calls_merge_and_removes_temp(monkeypatch, tmp_path):
    # Plan:
    # - Patch log_info to record calls.
    # - Patch merge_zip_archives to record calls.
    # - Patch shutil.rmtree to record calls.
    # - Patch get_base_file_name to return a predictable name.
    # - Patch results_output_subdirectory to tmp_path.
    # - Prepare a list of fake results file paths.
    # - Call finalize_results_zip_archives and assert all patches were called as expected.

    called = {
        "log_info": [],
        "merge_zip_archives": [],
        "rmtree": [],
    }

    # Patch log_info
    monkeypatch.setattr("results.log_info", lambda msg: called["log_info"].append(msg))
    # Patch merge_zip_archives
    def fake_merge_zip_archives(tempdir, outdir, base_name):
        called["merge_zip_archives"].append((tempdir, outdir, base_name))
    monkeypatch.setattr("results.merge_zip_archives", fake_merge_zip_archives)
    # Patch shutil.rmtree
    monkeypatch.setattr("shutil.rmtree", lambda path: called["rmtree"].append(path))
    # Patch get_base_file_name
    monkeypatch.setattr("results.get_base_file_name", lambda p: f"base_{os.path.basename(p)}")
    # Patch results_output_subdirectory
    monkeypatch.setattr("results.results_output_subdirectory", str(tmp_path))

    # Prepare fake results file paths
    results_file_paths = [
        str(tmp_path / "file1.txt"),
        str(tmp_path / "file2.txt"),
        str(tmp_path / "file3.txt"),
    ]

    # Call the function
    results.finalize_results_zip_archives(results_file_paths)

    # Check log_info called
    assert any("Finalizing the zip archives" in msg for msg in called["log_info"])
    # Check merge_zip_archives called for each file
    assert len(called["merge_zip_archives"]) == len(results_file_paths)
    for i, results_path in enumerate(results_file_paths):
        tempdir, outdir, base_name = called["merge_zip_archives"][i]
        assert tempdir == os.path.join(str(tmp_path), "temp")
        assert outdir == str(tmp_path)
        assert base_name == f"base_{os.path.basename(results_path)}"
    # Check rmtree called for tempdir
    assert called["rmtree"] == [os.path.join(str(tmp_path), "temp")]

def test_finalize_results_zip_archives_handles_empty_list(monkeypatch, tmp_path):
    # Patch log_info, merge_zip_archives, shutil.rmtree, get_base_file_name, results_output_subdirectory
    called = {
        "log_info": [],
        "merge_zip_archives": [],
        "rmtree": [],
    }
    monkeypatch.setattr("results.log_info", lambda msg: called["log_info"].append(msg))
    monkeypatch.setattr("results.merge_zip_archives", lambda *a, **k: called["merge_zip_archives"].append(a))
    monkeypatch.setattr("shutil.rmtree", lambda path: called["rmtree"].append(path))
    monkeypatch.setattr("results.get_base_file_name", lambda p: f"base_{os.path.basename(p)}")
    monkeypatch.setattr("results.results_output_subdirectory", str(tmp_path))

    results.finalize_results_zip_archives([])

    # Should still log and call rmtree, but not call merge_zip_archives
    assert any("Finalizing the zip archives" in msg for msg in called["log_info"])
    assert called["merge_zip_archives"] == []
    assert called["rmtree"] == [os.path.join(str(tmp_path), "temp")]

def test_finalize_results_zip_archives_propagates_merge_exception(monkeypatch, tmp_path):
    # Patch merge_zip_archives to raise, ensure exception propagates
    monkeypatch.setattr("results.log_info", lambda msg: None)
    def raise_exc(*a, **k):
        raise RuntimeError("merge failed")
    monkeypatch.setattr("results.merge_zip_archives", raise_exc)
    monkeypatch.setattr("shutil.rmtree", lambda path: None)
    monkeypatch.setattr("results.get_base_file_name", lambda p: "base")
    monkeypatch.setattr("results.results_output_subdirectory", str(tmp_path))

    with pytest.raises(RuntimeError, match="merge failed"):
        results.finalize_results_zip_archives([str(tmp_path / "file.txt")])
        
def test_get_results_file_path_returns_expected_path(monkeypatch, tmp_path):
    # Patch results_output_subdirectory to a known value
    monkeypatch.setattr("results.results_output_subdirectory", str(tmp_path))
    # Patch get_base_file_name to return a predictable name
    monkeypatch.setattr("results.get_base_file_name", lambda p: "mybase")
    definition_file_path = "/some/path/definition.txt"
    expected = str(tmp_path / "mybase_results.txt")
    actual = get_results_file_path(definition_file_path)
    assert actual == expected

def test_get_results_file_path_uses_base_file_name(monkeypatch, tmp_path):
    monkeypatch.setattr("results.results_output_subdirectory", str(tmp_path))
    # Use a real get_base_file_name implementation for this test
    # Simulate a definition file path
    definition_file_path = str(tmp_path / "foo_bar.txt")
    # The base file name should be 'foo_bar'
    expected = str(tmp_path / "foo_bar_results.txt")
    actual = get_results_file_path(definition_file_path)
    assert actual == expected

def test_get_results_file_path_with_empty_results_output_subdirectory(monkeypatch):
    monkeypatch.setattr("results.results_output_subdirectory", "")
    monkeypatch.setattr("results.get_base_file_name", lambda p: "basefile")
    definition_file_path = "somefile.txt"
    expected = "basefile_results.txt"
    actual = get_results_file_path(definition_file_path)
    assert actual == expected

def test_get_results_file_path_with_non_ascii_filename(monkeypatch, tmp_path):
    monkeypatch.setattr("results.results_output_subdirectory", str(tmp_path))
    definition_file_path = str(tmp_path / "café.txt")
    expected = str(tmp_path / "café_results.txt")
    actual = get_results_file_path(definition_file_path)
    assert actual == expected