# WarcSearcher
A Python tool that performs regex keyword searching iteratively over the contents of warc.gz files. Built using Python 3.11.4.

## Setup

[Work in progress]

* Ensure Python 3.11.4 is installed
* Acquire `WarcSearcher.py`, `config.ini`, and `requirements.txt`, and place them in a local directory.
* Using a terminal, navigate to the directory you placed the files in and install all required libraries: `pip install -r requirements.txt`
* Edit `config.ini` to provide the program with the following information:
  * `archives_directory` - (Required) The directory containing your warc.gz files. Subfolders will be searched recursively during program execution.
  * `definitions_directory` - (Required) The directory containing your regular expressions saved as .txt files. See the Definitions section for more details.
  * `findings_output_path` - (Optional) The directory for the results to be output to. Defaults to the current working directory if empty or invalid.
  * `zip_files_with_matches` - (Optional) Boolean indicating whether to add all files that yielded a match to a per-definition .zip file in the output directory. It is highly recommended to be used in order to better ascertain the context of matches - however, it can potentially consume large amounts of disk space depending on the regexes used to search. Use with discretion. Defaults to `False`.
  * `max_threads` - (Optional) The number of concurrent threads to search the records of the .WARC file(s) with. Use with discretion. Defaults to `4`.


### Arguments
* `zip` - (Optional) Adds all files that generated a match from the WARC to a per-definition zip file. This argument will override the value of `zip_files_with_matches` in `config.ini` if used. It is highly recommended to be used in order to better ascertain the context of matches - however, it can potentially consume large amounts of disk space depending on the regexes used to search. Use with discretion. 


### Definitions

WarcSearcher operates by reading in any number of "definition" files, and segregates the output by each definition. A definition is simply a .txt file in the user-defined `definitions_directory` that contains a regular expression. 

Here's a simple example of a `fruits.txt` that yields a match for any case-insensitive ocurrence of "Banana", "Apple", or "Orange":

`(?i)(banana|apple|orange)`

When executed, the program will output a `fruits_findings.txt` file in the output directory, containing all matches for the keywords prescribed by the regex found within the records of the WARC. Optionally, a `fruits_findings.zip` will also be output if `zip_files_with_matches = True` or the `zip` parameter was used, containing a copy of all files that yielded a match extracted from the WARC.


## Future Development
* Example definition files
* GUI?
* HTML output?

## Credits
This program was developed by the following members of the Lost Media Wiki:
* CrazyTom
* Bozo (InfiniteBlueGX)
