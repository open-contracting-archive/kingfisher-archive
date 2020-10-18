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

        self._data_md5 = None
        self._data_size = None
        self._cached_scrapy_log_file = None

    @property
    def local_directory(self):
        return os.path.join(self.data_directory, self.source_id, self.data_version.strftime("%Y%m%d_%H%M%S"))

    @property
    def remote_directory(self):
        return f'{self.source_id}/{self.data_version.year}/{self.data_version.month:02d}'

    def write_meta_data_file(self):
        data = {
            'database_id': self.database_id,
            'data_md5': self.local_directory_md5,
            'data_size': self.local_directory_bytes,
            'errors_count': self.errors_count,
        }
        file_descriptor, filename = tempfile.mkstemp(prefix='archive', suffix='.json')
        with open(filename, 'w') as file:
            json.dump(data, file, indent=2)
        os.close(file_descriptor)
        return filename

    @property
    def local_directory_md5(self):
        if self._data_md5 is not None:
            return self._data_md5

        cmd = 'find ' + self.local_directory + \
              ' -type f -exec md5sum {} + | awk \'{print $1}\' | sort | md5sum | awk \'{print $1}\''

        output = subprocess.check_output(cmd, universal_newlines=True, shell=True)

        self._data_md5 = output.strip()
        return self._data_md5

    @property
    def local_directory_bytes(self):
        if self._data_size is not None:
            return self._data_size

        output = subprocess.check_output(['du', '-sb', self.local_directory], universal_newlines=True)

        self._data_size = int(output.split('\t')[0])
        return self._data_size

    def get_data_files_exist(self):
        return os.path.isdir(self.local_directory)

    @property
    def scrapy_log_file(self):
        if self._cached_scrapy_log_file is not None:
            return self._cached_scrapy_log_file

        dir_to_search = os.path.join(self.logs_directory, self.source_id)
        if not os.path.isdir(dir_to_search):
            return

        for filename in os.listdir(dir_to_search):
            if filename.endswith(".log"):
                slf = ScrapyLogFile(os.path.join(dir_to_search, filename))
                if slf.does_match_date_version(self.data_version):
                    self._cached_scrapy_log_file = slf

        return self._cached_scrapy_log_file

    def has_errors_count(self):
        return bool(self.scrapy_log_file)

    @property
    def errors_count(self):
        return self.scrapy_log_file and self.scrapy_log_file.get_errors_sent_to_process_count()

    def write_data_file(self):
        file_descriptor, filename = tempfile.mkstemp(prefix='archive', suffix='.tar')
        os.close(file_descriptor)

        things_to_add = [self.local_directory]
        if self.scrapy_log_file:
            things_to_add.append(self.scrapy_log_file.name)

        subprocess.run(['tar', '-cf', filename, *things_to_add], check=True)
        subprocess.run(['lz4', filename, '{filename}.lz4'], check=True)

        os.unlink(filename)

        return f'{filename}.lz4'

    def delete_log_files(self):
        if self.scrapy_log_file and os.path.isfile(self.scrapy_log_file.name):
            os.remove(self.scrapy_log_file.name)
            if os.path.isfile(f'{self.scrapy_log_file.name}.stats'):
                os.remove(f'{self.scrapy_log_file.name}.stats')
            self._cached_scrapy_log_file = None
