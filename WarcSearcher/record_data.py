class RecordData:
  def __init__(self, parent_warc_gz_file, name, contents):
    self.parent_warc_gz_file = parent_warc_gz_file
    self.name = name
    self.contents = contents