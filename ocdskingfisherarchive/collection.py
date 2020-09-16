import json
import os
import subprocess
import tempfile
from ocdskingfisherarchive.scrapy_log_file import ScrapyLogFile


class Collection:

    def __init__(self, config, database_id, source_id, data_version):
        self.config = config
        self.database_id = database_id
        self.source_id = source_id
        self.data_version = data_version
        # The following attributes are filled by functions for caching purposes
        self.data_md5 = None
        self.data_size = None

    def write_meta_data_file(self):
        data = {
            'database_id': self.database_id,
            'data_md5': self.get_md5_of_data_folder(),
            'data_size': self.get_size_of_data_folder(),
        }
        file_descriptor, filename = tempfile.mkstemp(prefix='archive', suffix='.json')
        with open(filename, 'w') as file:
            json.dump(data, file, indent=2)
        os.close(file_descriptor)
        return filename

    def get_md5_of_data_folder(self):
        if self.data_md5 is not None:
            return self.data_md5

        dir = self.config.directory_data + '/' + self.source_id + '/' + self.data_version.strftime("%Y%m%d_%H%M%S")
        cmd = 'find '+dir+' -type f -exec md5sum {} + | awk \'{print $1}\' | sort | md5sum | awk \'{print $1}\''

        # Using Shell=True is not recommended when there are security implications,
        # but there is no user input here so it should be fine.
        # https://docs.python.org/3/library/subprocess.html#replacing-shell-pipeline
        # is the alternative and this is much more readable.
        output = subprocess.check_output(
            cmd,
            universal_newlines=True,
            shell=True
        )

        self.data_md5 = output.strip()
        return self.data_md5

    def get_size_of_data_folder(self):
        if self.data_size is not None:
            return self.data_size

        dir = self.config.directory_data + '/' + self.source_id + '/' + self.data_version.strftime("%Y%m%d_%H%M%S")
        args = ['du', '-sb', dir]

        output = subprocess.check_output(
            args,
            universal_newlines=True,
        )

        self.data_size = int(output.split('\t')[0])
        return self.data_size

    def _find_scrapyd_log_file(self):
        dir_to_search = os.path.join(self.config.directory_logs, self.source_id)
        for filename in os.listdir(dir_to_search):
            if filename.endswith(".log"):
                slf = ScrapyLogFile(os.path.join(dir_to_search, filename))
                if slf.does_match_date_version(self.data_version):
                    return os.path.join(dir_to_search, filename)

    def write_data_file(self):
        file_descriptor, filename = tempfile.mkstemp(prefix='archive', suffix='.tar')
        os.close(file_descriptor)

        things_to_add = [
            self.config.directory_data + '/' + self.source_id + '/' +
            self.data_version.strftime("%Y%m%d_%H%M%S")
        ]

        log_file_name = self._find_scrapyd_log_file()
        if log_file_name:
            things_to_add.append(log_file_name)

        command1 = 'tar -cf ' + filename + ' ' + ' '.join(things_to_add)
        return1 = os.system(command1)
        if return1 != 0:
            raise Exception(command1 + ' Got Return ' + str(return1))

        command2 = 'lz4 ' + filename + ' ' + filename+'.lz4'
        return2 = os.system(command2)
        if return2 != 0:
            raise Exception(command2 + ' Got Return ' + str(return2))

        return filename+'.lz4'

    def get_s3_directory(self):
        return self.source_id + '/' + str(self.data_version.year) + '/' + str(self.data_version.month).zfill(2)
