# WarcSearcher
A Python tool that performs regex keyword searching iteratively over the contents of warc.gz files.

## Setup

[Work in progress]

For now, you will need to provide filepaths for ARCHIVES_DIRECTORY and DEFINITIONS_DIRECTORY in the script itself. The former should be the directory containing your `warc.gz` files, and the latter should be the directory containing your regex definitions saved as individual text files.

### Parameters
`zip` - Saves all files that generated a match from the WARC to a zip file. Results are separated by definition. Disabled by default out of consideration for disk space, be mindful of this if you have many definitions to account for.


## Future Development
* Migrate configuration based global variables to a config file
* Example definition files
* GUI?
* HTML output?

## Credits
This program was developed by the following members of the Lost Media Wiki:
* CrazyTom
* Bozo (InfiniteBlueGX)
