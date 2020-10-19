import hashlib
import json
import os
import subprocess
import tempfile
from functools import partial

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

    def __str__(self):
        return os.path.join(self.source_id, self.data_version.strftime("%Y%m%d_%H%M%S"))

    @property
    def directory(self):
        return os.path.join(self.data_directory, self.source_id, self.data_version.strftime("%Y%m%d_%H%M%S"))

    @property
    def data_md5(self):
        if self._data_md5 is not None:
            return self._data_md5

        md5sum = hashlib.md5()
        for root, dirs, files in os.walk(self.directory):
            dirs.sort()
            for file in sorted(files):
                with open(os.path.join(root, file), 'rb') as f:
                    for chunk in iter(partial(f.read, 8192), b''):
                        md5sum.update(chunk)

        self._data_md5 = md5sum.hexdigest()
        return self._data_md5

    @property
    def data_size(self):
        if self._data_size is not None:
            return self._data_size

        self._data_size = sum(os.path.getsize(os.path.join(root, file))
                              for root, _, files in os.walk(self.directory) for file in files)
        return self._data_size

    def write_meta_data_file(self):
        data = {
            'data_md5': self.data_md5,
            'data_size': self.data_size,
            'errors_count': self.scrapy_log_file.errors_count,
        }
        file_descriptor, filename = tempfile.mkstemp(prefix='archive', suffix='.json')
        with open(filename, 'w') as file:
            json.dump(data, file, indent=2)
        os.close(file_descriptor)
        return filename

    def write_data_file(self):
        file_descriptor, filename = tempfile.mkstemp(prefix='archive', suffix='.tar')
        os.close(file_descriptor)

        things_to_add = [self.directory, self.scrapy_log_file.name]

        subprocess.run(['tar', '-cf', filename, *things_to_add], check=True)
        subprocess.run(['lz4', filename, f'{filename}.lz4'], check=True)

        os.unlink(filename)

        return f'{filename}.lz4'
