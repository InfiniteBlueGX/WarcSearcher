# WarcSearcher
A Python tool that performs regex keyword searches iteratively over the contents of local warc.gz files. Uses Python 3.12.2. (Currently in active development)

WarcSearcher is designed to facilitate the parsing of large web archives ([WARC files](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/)) with a variety of search criteria. It also aims to package the results for sharing in collaborative online settings.

WarcSearcher uses the [FastWARC](https://resiliparse.chatnoir.eu/en/latest/man/fastwarc.html) library for faster warc.gz processing compared to similar libraries such as warcio.

## Features

* Regex searching of all `response` records in a WARC
* Support for any number of regex "definitions" to search with - results output is segregated by definition
* Recursive searching of nested .zip, .7z, .rar, and .gz archives within a WARC
* Results output to .txt file per-definition
* Optional per-definition .zip output of all files from the WARC that yielded a match
* Multiprocessed warc.gz processing
* File name and contents searched (binary file data is skipped)

## Setup

[Work in progress]

* Ensure Python is installed and you are able to install packages via `pip install`.
* In order to process .rar files encountered while searching, WinRar must be installed and the path to the executable must be added to your System Path environment variable (i.e. `C:\Program Files\WinRAR`)
* Acquire WarcSearcher.py, config.ini, and requirements.txt, and place them in a local directory.
* Using a terminal, navigate to the directory you placed the files in and install all required libraries: `pip install -r requirements.txt`
* Edit config.ini to provide the program with the following required information:
  * `archives_directory` - (Required) The directory containing your warc.gz files. Subfolders will be searched recursively during program execution.
  * `definitions_directory` - (Required) The directory containing your regular expressions saved as .txt files. See the Definitions section for more details.
* Optionally, you may also want to modify these fields:
  * `findings_output_path` - (Optional) The directory for the results to be output to. Defaults to the current working directory if empty or invalid.
  * `zip_files_with_matches` - (Optional) Boolean indicating whether to add all files that yielded a match to a per-definition .zip file in the output directory. It is highly recommended to be used in order to better ascertain the context of matches - however, it can potentially consume large amounts of disk space depending on the regexes used to search. Use with discretion. Defaults to `False`.
  * `max_concurrent_archive_read_threads` - (Optional) The number of concurrent threads on the process responsible for reading in the records from the WARC files. Each .gz archive is assigned its own thread up to the maximum specified. Use with discretion. Defaults to `4`.
  * `max_concurrent_search_processes` - (Optional) The number of concurrent processes to perform the regex searches with. Defaults to `None` (which will assign one process per each available logical processor on your machine - set it lower if you want more headroom for multitasking).
  * `target_process_memory_bytes` - (Optional) The target process memory such that the PC's RAM isn't fully consumed when reading in multiple large WARC files. The program will wait for the search processes to clear out items from the queue until the process memory falls below the specified target. Defaults to `1000000000` (1GB).


### Command Line Arguments
* `zip` - (Optional) Adds all files that generated a match from the WARC to a per-definition zip file. This argument will override the value of `zip_files_with_matches` in config.ini if used. It is highly recommended to be used in order to better ascertain the context of matches - however, it can potentially consume large amounts of disk space depending on the regexes used to search. Use with discretion. 


### Definitions

WarcSearcher operates by reading in any number of regex "definition" files. A definition is simply a .txt file in the user-defined `definitions_directory` that contains a regular expression. 
For each definition, it performs the regex keyword search against all warc.gz files found in `archives_directory`, then segregates the output by each definition. 

Here's a simple example of the contents of a fruits.txt file that yields a match for any case-insensitive ocurrence of "Banana", "Apple", or "Orange":

`(?i)(banana|apple|orange)`

When executed, the program will output a fruits_findings.txt file in the output directory, containing all matches for the keywords prescribed by the regex found within the records of the WARC(s). Optionally, a fruits_findings.zip will also be output if `zip_files_with_matches = True` or the `zip` parameter was used, containing a copy of all files that yielded a match extracted from the WARC(s).

[More information about regexes](https://regextutorial.org/)


### Making an Executable (Optional)

In local testing, I've found that creating an executable speeds up the searching of large WARCs by about 25%. A standalone executable is planned for inclusion in the repo eventually, but you can create your own by navigating to the directory containing `WarcSearcher.py` and using PyInstaller:

* `pip install pyinstaller`
* `pyinstaller --onefile WarcSearcher.py`

The executable should be created in a `dist` folder - copy it to the directory containing your config.ini, then it should be ready for use. Be aware that it may be falsely flagged by antivirus software.

## Future Development
* Output by mimetype
* Performance optimizations
* Standalone executable
* GUI?
* HTML output?


## Contributing

Anyone is welcome to contribute to the repo! This is essentially a hobby project, but anyone is welcome to set up a pull request.

## Credits

* CrazyTom
* InfiniteBlueGX
