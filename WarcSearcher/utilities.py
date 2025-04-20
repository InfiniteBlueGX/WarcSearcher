import psutil


def find_regex_matches(input_string, regex_pattern) -> list:
    """Find all matches of the regex pattern in the input string."""
    return [match.group() for match in regex_pattern.finditer(input_string)]


def is_file_binary(file_data):
    """Returns True if the file is binary data, False if it is text."""
    text_chars = bytearray({7, 8, 9, 10, 12, 13, 27} | set(range(0x20, 0x100)) - {0x7f})
    first_1024_chars = file_data[:1024]
    return bool(first_1024_chars.translate(None, text_chars))


def get_total_memory_in_use(process):
    """Returns the total memory in use by the process and its subprocesses."""
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
    """Sanitizes a string intended to be usaed as a file name by removing web prefixes and invalid characters."""
    web_prefixes_removed = file_name.replace('http://', '').replace('https://', '').replace('www.', '')
    return web_prefixes_removed.translate(str.maketrans('','','\\/*?:"<>|'))