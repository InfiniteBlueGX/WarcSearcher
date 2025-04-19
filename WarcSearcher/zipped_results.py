import glob
import os
import zipfile


def create_temp_directory_for_zip_archives(results_output_subdirectory):
    """""Creates a temporary directory for zip archives in the results output subdirectory."""
    os.makedirs(os.path.join(results_output_subdirectory, "temp"))


def sanitize_file_name(file_name: str) -> str:
    """Sanitizes a file name by removing web prefixes and invalid characters."""
    web_prefixes_removed = file_name.replace('http://', '').replace('https://', '').replace('www.', '')
    return web_prefixes_removed.translate(str.maketrans('','','\\/*?:"<>|'))
    

def write_file_with_match_to_zip(file_data, file_name, zip_archive):
    """Writes a file with a match to the zip archive after ensuring a file with the same name is not already present in the zip archive."""
    reformatted_file_name = sanitize_file_name(file_name)
    if reformatted_file_name not in zip_archive.namelist():
        zip_archive.writestr(reformatted_file_name, file_data)


def merge_zip_archives(containing_dir, output_dir, definition_prefix):
    """Merges zip archives with the same prefix into a single zip archive. This is necessary because each search subprocess creates its own zip archive."""
    combined_zip = os.path.join(output_dir, f"{definition_prefix}.zip")
    added_files = set()

    for subdir, _, _ in os.walk(containing_dir):
        for file in glob.glob(os.path.join(subdir, f"{definition_prefix}*.zip")):
            with zipfile.ZipFile(file, 'r') as z1:
                with zipfile.ZipFile(combined_zip, 'a', compression=zipfile.ZIP_DEFLATED) as z2:
                    for file in z1.namelist():
                        if file not in added_files:
                            z2.writestr(file, z1.read(file))
                            added_files.add(file)