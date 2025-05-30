import re
import sys
from utilities import *
import io
import zipfile

def test_find_regex_matches_multiple():
    pattern = re.compile(r'\d+')
    input_string = "abc123def456ghi789"
    result = find_regex_matches(input_string, pattern)
    assert result == ['123', '456', '789']

def test_find_regex_matches_none():
    pattern = re.compile(r'xyz')
    input_string = "abc123def456"
    result = find_regex_matches(input_string, pattern)
    assert result == []

def test_find_regex_matches_overlapping():
    pattern = re.compile(r'aba')
    input_string = "ababa"
    result = find_regex_matches(input_string, pattern)
    assert result == ['aba']

def test_find_regex_matches_words():
    pattern = re.compile(r'\w+')
    input_string = "word1 word2, word3!"
    result = find_regex_matches(input_string, pattern)
    assert result == ['word1', 'word2', 'word3']

def test_find_regex_matches_empty_string():
    pattern = re.compile(r'\d+')
    input_string = ""
    result = find_regex_matches(input_string, pattern)
    assert result == []

def test_find_regex_matches_empty_pattern():
    pattern = re.compile(r'')
    input_string = "abc"
    result = find_regex_matches(input_string, pattern)
    # Empty pattern matches at every position, including start and end
    assert result == ['', '', '', '']

def test_get_base_file_name_simple():
    assert get_base_file_name("folder/file.txt") == "file"

def test_get_base_file_name_no_extension():
    assert get_base_file_name("folder/file") == "file"

def test_get_base_file_name_multiple_dots():
    assert get_base_file_name("folder/archive.tar.gz") == "archive.tar"

def test_get_base_file_name_hidden_file():
    assert get_base_file_name("/path/.hiddenfile") == ".hiddenfile"

def test_get_base_file_name_windows_path():
    if sys.platform.startswith("win"):
        assert get_base_file_name(r"C:\Users\user\document.pdf") == "document"

def test_get_base_file_name_linux_path():
    if sys.platform.startswith("linux"):
        assert get_base_file_name("/home/user/document.pdf") == "document"

def test_get_base_file_name_trailing_slash():
    assert get_base_file_name("folder/") == ""

def test_get_base_file_name_empty_string():
    assert get_base_file_name("") == ""

def test_is_file_binary_with_text_data():
    # ASCII text data
    text_data = b"Hello, this is a plain text file.\nIt has multiple lines.\n"
    assert is_file_binary(text_data) is False

def test_is_file_binary_with_utf8_text():
    # UTF-8 encoded text with non-ASCII characters
    utf8_text = "Café résumé naïve".encode("utf-8")
    assert is_file_binary(utf8_text) is False

def test_is_file_binary_with_binary_data():
    # Typical binary file header (e.g., PNG)
    binary_data = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"
    assert is_file_binary(binary_data) is True

def test_is_file_binary_with_null_bytes():
    # Data with null bytes, typical in binaries
    binary_data = b"\x00\x01\x02\x03\x04\x05"
    assert is_file_binary(binary_data) is True

def test_is_file_binary_with_mixed_content():
    # Text with some binary bytes
    mixed_data = b"Hello world\x00\x01"
    assert is_file_binary(mixed_data) is True

def test_is_file_binary_with_empty_data():
    # Empty file should not be considered binary
    empty_data = b""
    assert is_file_binary(empty_data) is False

def test_is_file_binary_with_long_text():
    # Long text data, more than 1024 bytes
    long_text = b"A" * 2048
    assert is_file_binary(long_text) is False

def test_is_file_binary_with_long_binary():
    # Long binary data, more than 1024 bytes
    long_binary = b"\x00\xff" * 1024
    assert is_file_binary(long_binary) is True

def test_get_total_ram_used_percent_is_int(monkeypatch):
    class DummyVMem:
        percent = 42.7
    monkeypatch.setattr("psutil.virtual_memory", lambda: DummyVMem())
    result = get_total_ram_used_percent()
    assert isinstance(result, int)

def test_get_total_ram_used_percent_rounds_down(monkeypatch):
    class DummyVMem:
        percent = 55.9
    monkeypatch.setattr("psutil.virtual_memory", lambda: DummyVMem())
    result = get_total_ram_used_percent()
    assert result == 55

def test_get_total_ram_used_percent_rounds_up(monkeypatch):
    class DummyVMem:
        percent = 99.99
    monkeypatch.setattr("psutil.virtual_memory", lambda: DummyVMem())
    result = get_total_ram_used_percent()
    assert result == 99

def test_get_total_ram_used_percent_zero(monkeypatch):
    class DummyVMem:
        percent = 0.0
    monkeypatch.setattr("psutil.virtual_memory", lambda: DummyVMem())
    result = get_total_ram_used_percent()
    assert result == 0

def test_get_total_ram_used_percent_hundred(monkeypatch):
    class DummyVMem:
        percent = 100.0
    monkeypatch.setattr("psutil.virtual_memory", lambda: DummyVMem())
    result = get_total_ram_used_percent()
    assert result == 100

def test_sanitize_file_name_string_removes_web_prefixes():
    assert sanitize_file_name_string("http://example.com/file.txt") == "example.comfile.txt"
    assert sanitize_file_name_string("https://example.com/file.txt") == "example.comfile.txt"
    assert sanitize_file_name_string("www.example.com/file.txt") == "example.comfile.txt"
    assert sanitize_file_name_string("https://www.example.com/file.txt") == "example.comfile.txt"

def test_sanitize_file_name_string_removes_invalid_characters():
    assert sanitize_file_name_string("file:name?.txt") == "filename.txt"
    assert sanitize_file_name_string("file*name|.txt") == "filename.txt"
    assert sanitize_file_name_string("file<name>.txt") == "filename.txt"
    assert sanitize_file_name_string("file/name\\test.txt") == "filenametest.txt"

def test_sanitize_file_name_string_combined_cases():
    assert sanitize_file_name_string("https://www.example.com/fi*le:na?me.txt") == "example.comfilename.txt"
    assert sanitize_file_name_string("http://www.site.com/te|st<doc>.pdf") == "site.comtestdoc.pdf"

def test_sanitize_file_name_string_no_changes_needed():
    assert sanitize_file_name_string("filename.txt") == "filename.txt"
    assert sanitize_file_name_string("simple_name") == "simple_name"

def test_sanitize_file_name_string_empty_string():
    assert sanitize_file_name_string("") == ""

def test_sanitize_file_name_string_only_invalid_chars():
    assert sanitize_file_name_string("\\/*?:\"<>|") == ""

def test_sanitize_file_name_string_only_web_prefix():
    assert sanitize_file_name_string("http://") == ""
    assert sanitize_file_name_string("https://") == ""
    assert sanitize_file_name_string("www.") == ""

def test_add_file_to_zip_archive_adds_file():
    file_name = "test.txt"
    file_data = b"Hello, world!"
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED) as zf:
        add_file_to_zip_archive(file_name, file_data, zf)
        assert sanitize_file_name_string(file_name) in zf.namelist()
        assert zf.read(sanitize_file_name_string(file_name)) == file_data

def test_add_file_to_zip_archive_does_not_duplicate():
    file_name = "duplicate.txt"
    file_data1 = b"First version"
    file_data2 = b"Second version"
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED) as zf:
        add_file_to_zip_archive(file_name, file_data1, zf)
        add_file_to_zip_archive(file_name, file_data2, zf)
        assert zf.namelist().count(sanitize_file_name_string(file_name)) == 1
        assert zf.read(sanitize_file_name_string(file_name)) == file_data1

def test_add_file_to_zip_archive_sanitizes_filename():
    file_name = "http://www.example.com/fi*le:na?me.txt"
    file_data = b"data"
    sanitized = sanitize_file_name_string(file_name)
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED) as zf:
        add_file_to_zip_archive(file_name, file_data, zf)
        assert sanitized in zf.namelist()
        assert zf.read(sanitized) == file_data

def test_add_file_to_zip_archive_empty_file_data():
    file_name = "empty.txt"
    file_data = b""
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED) as zf:
        add_file_to_zip_archive(file_name, file_data, zf)
        assert sanitize_file_name_string(file_name) in zf.namelist()
        assert zf.read(sanitize_file_name_string(file_name)) == b""

def test_add_file_to_zip_archive_empty_filename():
    file_name = ""
    file_data = b"content"
    sanitized = sanitize_file_name_string(file_name)
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED) as zf:
        add_file_to_zip_archive(file_name, file_data, zf)
        # Empty string as filename is technically allowed in zip, but not recommended
        assert sanitized in zf.namelist()
        assert zf.read(sanitized) == file_data

def create_zip_with_files_helper(zip_path, files_dict):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in files_dict.items():
            zf.writestr(name, data)

def test_merge_zip_archives_merges_files(tmp_path):
    parent_dir = tmp_path / "parent"
    output_dir = tmp_path / "output"
    parent_dir.mkdir()
    output_dir.mkdir()
    sub1 = parent_dir / "sub1"
    sub2 = parent_dir / "sub2"
    sub1.mkdir()
    sub2.mkdir()
    zip_name = "archive"
    zip1_path = sub1 / f"{zip_name}.zip"
    zip2_path = sub2 / f"{zip_name}.zip"
    files1 = {"a.txt": b"foo", "b.txt": b"bar"}
    files2 = {"b.txt": b"bar", "c.txt": b"baz"}
    create_zip_with_files_helper(zip1_path, files1)
    create_zip_with_files_helper(zip2_path, files2)

    merge_zip_archives(str(parent_dir), str(output_dir), zip_name)

    combined_zip = output_dir / f"{zip_name}.zip"
    with zipfile.ZipFile(combined_zip, "r") as zf:
        names = set(zf.namelist())
        assert names == {"a.txt", "b.txt", "c.txt"}
        assert zf.read("a.txt") == b"foo"
        assert zf.read("b.txt") == b"bar"
        assert zf.read("c.txt") == b"baz"

# def test_merge_zip_archives_skips_duplicates(tmp_path):
#     parent_dir = tmp_path / "parent"
#     output_dir = tmp_path / "output"
#     parent_dir.mkdir()
#     output_dir.mkdir()
#     sub1 = parent_dir / "sub1"
#     sub2 = parent_dir / "sub2"
#     sub1.mkdir()
#     sub2.mkdir()
#     zip_name = "archive"
#     zip1_path = sub1 / f"{zip_name}.zip"
#     zip2_path = sub2 / f"{zip_name}.zip"
#     files1 = {"file.txt": b"data1"}
#     files2 = {"file.txt": b"data2"}
#     create_zip_with_files_helper(zip1_path, files1)
#     create_zip_with_files_helper(zip2_path, files2)

#     merge_zip_archives(str(parent_dir), str(output_dir), zip_name)

#     combined_zip = output_dir / f"{zip_name}.zip"
#     with zipfile.ZipFile(combined_zip, "r") as zf:
#         # Only one file.txt, and it should be from the first zip encountered
#         assert zf.namelist().count("file.txt") == 1
#         assert zf.read("file.txt") == b"data1"

def test_merge_zip_archives_handles_no_archives(tmp_path):
    parent_dir = tmp_path / "parent"
    output_dir = tmp_path / "output"
    parent_dir.mkdir()
    output_dir.mkdir()
    zip_name = "archive"

    merge_zip_archives(str(parent_dir), str(output_dir), zip_name)

    combined_zip = output_dir / f"{zip_name}.zip"
    # Should not create a zip if there are no archives
    assert not combined_zip.exists()

def test_merge_zip_archives_merges_multiple_named_archives(tmp_path):
    parent_dir = tmp_path / "parent"
    output_dir = tmp_path / "output"
    parent_dir.mkdir()
    output_dir.mkdir()
    sub1 = parent_dir / "sub1"
    sub2 = parent_dir / "sub2"
    sub1.mkdir()
    sub2.mkdir()
    zip_name = "archive"
    zip1_path = sub1 / f"{zip_name}_part1.zip"
    zip2_path = sub2 / f"{zip_name}_part2.zip"
    files1 = {"foo.txt": b"foo"}
    files2 = {"bar.txt": b"bar"}
    create_zip_with_files_helper(zip1_path, files1)
    create_zip_with_files_helper(zip2_path, files2)

    merge_zip_archives(str(parent_dir), str(output_dir), zip_name)

    combined_zip = output_dir / f"{zip_name}.zip"
    with zipfile.ZipFile(combined_zip, "r") as zf:
        names = set(zf.namelist())
        assert names == {"foo.txt", "bar.txt"}
        assert zf.read("foo.txt") == b"foo"
        assert zf.read("bar.txt") == b"bar"