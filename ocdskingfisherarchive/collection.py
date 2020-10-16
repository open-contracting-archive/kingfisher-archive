import json
import os
import subprocess
import tempfile

from ocdskingfisherarchive.scrapy_log_file import ScrapyLogFile


class Collection:
    def __init__(self, database_id, source_id, data_version, data_directory='', logs_directory=''):
        self.database_id = database_id
        self.source_id = source_id
        self.data_version = data_version
        self.data_directory = data_directory
        self.logs_directory = logs_directory

        # The following attributes are filled by functions for caching purposes
        self._data_md5 = None
        self._data_size = None
        self._cached_scrapy_log_file_name = None
        self._cached_scrapy_log_file = None

    def _get_data_dir_name(self):
        return os.path.join(self.data_directory, self.source_id, self.data_version.strftime("%Y%m%d_%H%M%S"))

    def write_meta_data_file(self):
        data = {
            'database_id': self.database_id,
            'data_md5': self.get_md5_of_data_folder(),
            'data_size': self.get_size_of_data_folder(),
            'scrapy_log_file_found': self.has_errors_count(),
            'errors_count': self.get_errors_count(),
        }
        file_descriptor, filename = tempfile.mkstemp(prefix='archive', suffix='.json')
        with open(filename, 'w') as file:
            json.dump(data, file, indent=2)
        os.close(file_descriptor)
        return filename

    def get_md5_of_data_folder(self):
        if self._data_md5 is not None:
            return self._data_md5

        cmd = 'find ' + self._get_data_dir_name() + \
              ' -type f -exec md5sum {} + | awk \'{print $1}\' | sort | md5sum | awk \'{print $1}\''

        # Using Shell=True is not recommended when there are security implications,
        # but there is no user input here so it should be fine.
        # https://docs.python.org/3/library/subprocess.html#replacing-shell-pipeline
        # is the alternative and this is much more readable.
        output = subprocess.check_output(cmd, universal_newlines=True, shell=True)

        self._data_md5 = output.strip()
        return self._data_md5

    def get_size_of_data_folder(self):
        if self._data_size is not None:
            return self._data_size

        output = subprocess.check_output(['du', '-sb', self._get_data_dir_name()], universal_newlines=True)

        self._data_size = int(output.split('\t')[0])
        return self._data_size

    def get_data_files_exist(self):
        # It must exist and be a directory
        return os.path.isdir(self._get_data_dir_name())

    def _cache_scrapyd_log_file_info(self):
        if self._cached_scrapy_log_file_name is not None:
            return

        dir_to_search = os.path.join(self.logs_directory, self.source_id)
        if not os.path.isdir(dir_to_search):
            return
        for filename in os.listdir(dir_to_search):
            if filename.endswith(".log"):
                slf = ScrapyLogFile(os.path.join(dir_to_search, filename))
                if slf.does_match_date_version(self.data_version):
                    self._cached_scrapy_log_file = slf
                    self._cached_scrapy_log_file_name = os.path.join(dir_to_search, filename)
                    return

    @property
    def scrapy_log_file(self):
        self._cache_scrapyd_log_file_info()
        return self._cached_scrapy_log_file

    @property
    def scrapy_log_file_name(self):
        self._cache_scrapyd_log_file_info()
        return self._cached_scrapy_log_file_name

    def is_subset(self):
        return self.scrapy_log_file and self.scrapy_log_file.is_subset()

    def has_errors_count(self):
        return bool(self.scrapy_log_file)

    def get_errors_count(self):
        return self.scrapy_log_file and self.scrapy_log_file.get_errors_sent_to_process_count()

    def write_data_file(self):
        file_descriptor, filename = tempfile.mkstemp(prefix='archive', suffix='.tar')
        os.close(file_descriptor)

        things_to_add = [
            self._get_data_dir_name()
        ]

        if self.scrapy_log_file_name:
            things_to_add.append(self.scrapy_log_file_name)

        subprocess.run(['tar', '-cf', filename, *things_to_add], check=True)
        subprocess.run(['lz4', filename, '{filename}.lz4'], check=True)

        os.unlink(filename)

        return f'{filename}.lz4'

    def get_s3_directory(self):
        return f'{self.source_id}/{self.data_version.year}/{self.data_version.month:02d}'

    def delete_data_files(self):
        if self.get_data_files_exist():
            subprocess.run(['sudo', '-u', 'ocdskfs', '/bin/rm', '-rf', self._get_data_dir_name()], check=True)

    def delete_log_files(self):
        if self.scrapy_log_file_name and os.path.isfile(self.scrapy_log_file_name):
            subprocess.run(['sudo', '-u', 'ocdskfs', '/bin/rm', '-f', self.scrapy_log_file_name], check=True)
            if os.path.isfile(f'{self.scrapy_log_file_name}.stats'):
                subprocess.run(['sudo', '-u', 'ocdskfs', '/bin/rm', '-f', f'{self.scrapy_log_file_name}.stats'],
                               check=True)
            self._cached_scrapy_log_file_name = None
            self._cached_scrapy_log_file = None
