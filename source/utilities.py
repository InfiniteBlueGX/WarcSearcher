import glob
import os
import re
import zipfile
import psutil


def find_regex_matches(input_string: str, regex_pattern: re.Pattern) -> list:
    """Finds all matches of the regex pattern in the input string and returns them as a list."""
    return [match.group() for match in regex_pattern.finditer(input_string)]


def get_base_file_name(file_path: str) -> str:
    """Returns the base name of a file without its extension."""
    return os.path.splitext(os.path.basename(file_path))[0]


def is_file_binary(file_data) -> bool:
    """Returns True if the file is binary data based on the first 1024 characters."""
    text_chars = bytearray({7, 8, 9, 10, 12, 13, 27} | set(range(0x20, 0x100)) - {0x7f})
    first_1024_chars = file_data[:1024]
    return bool(first_1024_chars.translate(None, text_chars))


def get_total_ram_used_percent() -> int:
    """Returns the percentage of RAM currently in use on the machine as an integer."""
    return int(psutil.virtual_memory().percent)


def sanitize_file_name_string(file_name: str) -> str:
    """Sanitizes a string intended to be used as a file name by removing web prefixes and invalid characters."""
    web_prefixes_removed = file_name.replace('http://', '').replace('https://', '').replace('www.', '')
    return web_prefixes_removed.translate(str.maketrans('','','\\/*?:"<>|'))


def add_file_to_zip_archive(file_name: str, file_data, zip_archive: zipfile.ZipFile):
    """Adds a file to an existing zip archive after ensuring a file with the same name is not already present in the archive."""
    sanitized_file_name = sanitize_file_name_string(file_name)
    if sanitized_file_name not in zip_archive.namelist():
        zip_archive.writestr(sanitized_file_name, file_data)


def merge_zip_archives(parent_dir: str, output_dir: str, archive_name: str):
    """
    Merges identically named zip archives in subdirectories of the parent directory into a single zip archive in the output directory. 
    """
    combined_zip = os.path.join(output_dir, f"{archive_name}.zip")
    added_files = set()

    for subdir, _, _ in os.walk(parent_dir):
        for file in glob.glob(os.path.join(subdir, f"{archive_name}*.zip")):
            with zipfile.ZipFile(file, 'r') as zip1:
                with zipfile.ZipFile(combined_zip, 'a', compression=zipfile.ZIP_DEFLATED) as zip2:
                    for file in zip1.namelist():
                        if file not in added_files:
                            zip2.writestr(file, zip1.read(file))
                            added_files.add(file)