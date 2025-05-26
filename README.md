# WarcSearcher

![GitHub Actions](https://github.com/InfiniteBlueGX/WarcSearcher/actions/workflows/test-warcsearcher.yml/badge.svg)

A Python program that performs regular expression searches over the contents of any number of local WARC.gz (Web Archive) files. [WARC specification](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/)

WarcSearcher is designed to perform multiple simultaneous regex queries against records read from the WARC.gz files. These queries are supplied as "Definitions" - text files containing a regex to search with.

WarcSearcher will output the results in text format: one results file for each definition. Optionally, it can extract any record from the WARC.gz that yielded a match and save the file to a zip archive in the results folder.

![WarcSearcher Diagram](diagram.png)

## Features

* Searches all `response` records from any number of WARC.gz files against any number of regex definitions
* Multiprocessed regex searching and results output; user configurable
* Extracts files from any WARC.gz that produced a match to a .zip archive
* Optionally skips searching binary file data

## Setup

* Ensure Python is installed and download/clone the repository.
* Using a terminal, navigate to the repository and install all required libraries: `pip install -r requirements.txt`
* Configure `config.ini` at the root of the repository (see section below).
* Run `main.py` in the `source` folder.

## Configuration

WarcSearcher can be configured with the `config.ini` file at the root of the repository:

### Required Variables

* `WARC_GZ_ARCHIVES_DIRECTORY` - The directory containing the WARC.gz files.
* `SEARCH_REGEX_DEFINITIONS_DIRECTORY` - The directory where the .txt files containing regular expressions to search with are located.
* `RESULTS_OUTPUT_DIRECTORY` - The directory where the results are output to. The .txt and .zip results files for the execution will be stored in a timestamped folder within this directory.

### Optional Variables

* `ZIP_FILES_WITH_MATCHES` - Default: `False`. When set to True, any WARC record that produced a match for a definition will be extracted from the WARC.gz file and saved to a zip archive, named similarly to the results text file.
* `MAX_CONCURRENT_SEARCH_PROCESSES` - Default: `None`. The number of concurrent processes to perform the regex searches with. If in excess of the number of logical processors available on the PC, the value reverts to the number of logical processors. These processes are independent of the main process responsible for reading the WARC records. Setting this higher may not necessarily perform the search faster - execution time is highly variable depending on the PC's number of logical processors, the complexity of regexes used, and the size of the WARC.gz files to be searched. With less complex regexes, a lower value may improve execution time slightly. However, if you are frequently hitting the maximum RAM usage value (see below), increasing this value as high as possible is recommended.
* `MAX_RAM_USAGE_PERCENT` - Default: `90` (percent). Maximum percentage of how much RAM should be in use on the PC while WarcSearcher is executing. This is a failsafe to ensure that RAM is not exhausted if the search processes cannot keep up with the pace of WARC records being read in by the main process. WarcSearcher will pause reading records for 10 seconds if the current percentage of used RAM exceeds this value, in order to allow the search processes time to process records already in the search queue.
* `SEARCH_BINARY_FILES` - Default: `False`. Boolean indicating whether records containing non-human-readable binary file data (images, video, music, etc) should be searched. Setting this to `True` may greatly increase search time.
