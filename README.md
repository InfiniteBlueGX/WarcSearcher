# WarcSearcher

A Python tool that performs regex keyword searches iteratively over the contents of local warc.gz files. Uses Python 3.12.2. (Currently in active development)

WarcSearcher is designed to facilitate the parsing of large web archives ([WARC files](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/)) with a variety of search criteria. It also aims to package the results for sharing in collaborative online settings.

WarcSearcher uses the [FastWARC](https://resiliparse.chatnoir.eu/en/latest/man/fastwarc.html) library for faster warc.gz processing compared to similar libraries such as warcio.

## Features

* Regex searching of all `response` records in a WARC
* Support for any number of regex "definitions" to search with - results output is segregated by definition
* Results output to .txt file per-definition
* Optional per-definition .zip output of all files from the WARC that yielded a match
* Multiprocessed warc.gz processing
* File name and contents searched (binary file data is skipped)

## Setup

[Work in progress]

* Ensure Python is installed and you are able to install packages via `pip install`.
* Acquire WarcSearcher.py, config.ini, and requirements.txt, and place them in a local directory.
* Using a terminal, navigate to the directory you placed the files in and install all required libraries: `pip install -r requirements.txt`
* Edit config.ini to provide the program with the following required information:
  * `archives_directory` - (Required) The directory containing your warc.gz files. Subfolders will be searched recursively during program execution.
  * `definitions_directory` - (Required) The directory containing your regular expressions saved as .txt files. See the Definitions section for more details.
  * `findings_output_path` - (Optional) The directory for the results to be output to. Defaults to the current working directory if empty or invalid.
* Optionally, you may also want to modify these fields:
  * `zip_files_with_matches` - (Optional) Boolean indicating whether to add all files that yielded a match to a per-definition .zip file in the output directory. It is highly recommended to be used in order to better ascertain the context of matches - however, it can potentially consume large amounts of disk space depending on the regexes used to search. Use with discretion. Defaults to `False`.
  * `max_concurrent_archive_read_threads` - (Optional) The number of concurrent threads on the process responsible for reading in the records from the WARC files. Each .gz archive is assigned its own thread up to the maximum specified. Use with discretion. Defaults to `4`.
  * `max_concurrent_search_processes` - (Optional) The number of concurrent processes to perform the regex searches with. Defaults to `None` (which will assign one process per each available logical processor on your machine - set it lower if you want more headroom for multitasking).
  * `MAX_RAM_USAGE_BYTES` - (Optional) The target process memory such that the PC's RAM isn't fully consumed when reading in multiple large WARC files. The program will wait for the search processes to clear out items from the queue until the process memory falls below the specified target. Defaults to `2000000000` (1GB).
