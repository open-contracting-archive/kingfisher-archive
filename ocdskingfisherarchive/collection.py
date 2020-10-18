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

        self.scrapy_log_file = ScrapyLogFile.find(logs_directory, source_id, data_version)

        self._data_md5 = None
        self._data_size = None

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
            'errors_count': self.scrapy_log_file.errors_count,
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

    def write_data_file(self):
        file_descriptor, filename = tempfile.mkstemp(prefix='archive', suffix='.tar')
        os.close(file_descriptor)

        things_to_add = [self.local_directory, self.scrapy_log_file.name]

        subprocess.run(['tar', '-cf', filename, *things_to_add], check=True)
        subprocess.run(['lz4', filename, '{filename}.lz4'], check=True)

        os.unlink(filename)

        return f'{filename}.lz4'
