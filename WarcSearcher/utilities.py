import glob
import os
import zipfile
import psutil


def find_regex_matches(input_string, regex_pattern) -> list:
    """Find all matches of the regex pattern in the input string."""
    return [match.group() for match in regex_pattern.finditer(input_string)]


def is_file_binary(file_data) -> bool:
    """Returns True if the file is binary data, False if it is text."""
    text_chars = bytearray({7, 8, 9, 10, 12, 13, 27} | set(range(0x20, 0x100)) - {0x7f})
    first_1024_chars = file_data[:1024]
    return bool(first_1024_chars.translate(None, text_chars))


def get_total_memory_in_use(process: psutil.Process) -> int:
    """Returns the total memory in use by the WarcSearcher process and its subprocesses."""
    mem_info = process.memory_info()
    resident_set_size_memory = mem_info.rss

    subprocesses = process.children(recursive=True)
    for subprocess in subprocesses:
        mem_info = subprocess.memory_info()
        resident_set_size_memory += mem_info.rss

    return resident_set_size_memory


def get_total_ram_bytes_rounded() -> int:
    """Returns the total RAM of the machine in bytes, rounded down to the nearest GB."""
    total_ram = psutil.virtual_memory().total
    return (total_ram // (1024 ** 3)) * (1024 ** 3)


def sanitize_file_name(file_name: str) -> str:
    """Sanitizes a string intended to be used as a file name by removing web prefixes and invalid characters."""
    web_prefixes_removed = file_name.replace('http://', '').replace('https://', '').replace('www.', '')
    return web_prefixes_removed.translate(str.maketrans('','','\\/*?:"<>|'))


def add_file_to_zip_archive(file_name, file_data, zip_archive):
    """Adds a file to an existing zip archive after ensuring a file with the same name is not already present in the archive."""
    sanitized_file_name = sanitize_file_name(file_name)
    if sanitized_file_name not in zip_archive.namelist():
        zip_archive.writestr(sanitized_file_name, file_data)


def merge_zip_archives(parent_dir, output_dir, archive_name):
    """
    Merges zip archives with the same name in subdirectories of the parent directory into a single zip archive. 
    This is necessary because each search process creates independent zip archives.
    """
    combined_zip = os.path.join(output_dir, f"{archive_name}.zip")
    added_files = set()

    for subdir, _, _ in os.walk(parent_dir):
        for file in glob.glob(os.path.join(subdir, f"{archive_name}*.zip")):
            with zipfile.ZipFile(file, 'r') as z1:
                with zipfile.ZipFile(combined_zip, 'a', compression=zipfile.ZIP_DEFLATED) as z2:
                    for file in z1.namelist():
                        if file not in added_files:
                            z2.writestr(file, z1.read(file))
                            added_files.add(file)