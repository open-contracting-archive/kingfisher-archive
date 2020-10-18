class ArchivedCollection:
    def __init__(self, metadata, year, month):
        self.metadata = metadata
        self.year = year
        self.month = month

    @property
    def data_md5(self):
        return self.metadata and self.metadata.get('data_md5')

    @property
    def data_size(self):
        return self.metadata and self.metadata.get('data_size')

    def has_errors_count(self):
        return self.metadata and self.metadata.get('errors_count') is not None

    @property
    def errors_count(self):
        return self.metadata and self.metadata.get('errors_count')
