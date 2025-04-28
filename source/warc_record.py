class WarcRecord:
  def __init__(self, parent_warc_gz_file: str, name: str, contents: bytes):
    self.parent_warc_gz_file: str = parent_warc_gz_file
    self.name: str = name
    self.contents: bytes = contents