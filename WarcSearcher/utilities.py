import psutil


def find_regex_matches(input_string, regex_pattern):
    return [match.group() for match in regex_pattern.finditer(input_string)]


def is_file_binary(file_data):
    """ Check if the file data is binary or text. """
    text_chars = bytearray({7, 8, 9, 10, 12, 13, 27} | set(range(0x20, 0x100)) - {0x7f})
    first_1024_chars = file_data[:1024]
    return bool(first_1024_chars.translate(None, text_chars))



def get_total_memory_in_use(process):
    mem_info = process.memory_info()
    resident_set_size_memory = mem_info.rss

    subprocesses = process.children(recursive=True)
    for subprocess in subprocesses:
        mem_info = subprocess.memory_info()
        resident_set_size_memory += mem_info.rss

    return resident_set_size_memory



def get_total_ram_bytes_rounded() -> int:
    total_ram = psutil.virtual_memory().total
    # Round down to the nearest GB and convert back to bytes
    return (total_ram // (1024 ** 3)) * (1024 ** 3)