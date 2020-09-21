import json


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


class ArchivedCollection:

    @staticmethod
    def load_exact(s3, source_id, data_version):
        """Loads an archive from S3 for source && exact year/month, if it exists"""
        return ArchivedCollection._load(s3, source_id, data_version.year, data_version.month)

    @staticmethod
    def load_latest(s3, source_id, data_version):
        """Loads an archive from S3 for source && the latest year/month up to the one passed, if any exist"""
        data = s3.get_years_and_months_for_source(source_id)
        year, month = _find_latest_year_month_to_load(data, data_version.year, data_version.month)
        if year and month:
            return ArchivedCollection._load(s3, source_id, year, month)

    @staticmethod
    def _load(s3, source_id, year, month):
        remote_filename = source_id + '/' + str(year) + '/' + \
                          str(month).zfill(2) + '/metadata.json'
        filename = s3.get_file(remote_filename)
        if filename:
            with open(filename) as fp:
                return ArchivedCollection(json.load(fp), year, month)
            # TODO delete the file to save our /tmp space

    def __init__(self, data, year, month):
        self._data = data
        self.year = year
        self.month = month

    def get_data_md5(self):
        return self._data and self._data.get('data_md5')

    def get_data_size(self):
        return self._data and self._data.get('data_size')

    def has_errors_count(self):
        return self._data and self._data.get('scrapy_log_file_found')

    def get_errors_count(self):
        return self._data and self._data.get('errors_count')
