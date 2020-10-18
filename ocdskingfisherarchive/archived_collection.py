import json
import os


def _find_latest_year_month_to_load(data, year, month):
    while year > 2010:
        if data.get(year, {}).get(month):
            return year, month
        if month > 1:
            month = month - 1
        else:
            year = year - 1
            month = 12
    return None, None


def load_exact(s3, source_id, data_version):
    """
    Loads an archive from S3 for source && exact year/month, if it exists.
    """
    return _load(s3, source_id, data_version.year, data_version.month)


def load_latest(s3, source_id, data_version):
    """
    Loads an archive from S3 for source && the latest year/month up to the one passed, if any exist.
    """
    data = s3.get_years_and_months_for_source(source_id)
    year, month = _find_latest_year_month_to_load(data, data_version.year, data_version.month)
    if year and month:
        return _load(s3, source_id, year, month)


def _load(s3, source_id, year, month):
    remote_filename = f'{source_id}/{year}/{month:02d}/metadata.json'
    filename = s3.get_file(remote_filename)
    if filename:
        with open(filename) as fp:
            archived_collection = ArchivedCollection(json.load(fp), year, month)
        os.unlink(filename)
        return archived_collection


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
