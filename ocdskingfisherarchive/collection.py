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
        self._data_md5 = None
        self._data_size = None
        self._scrapy_log_file_name = None
        self._scrapy_log_file = None

    def write_meta_data_file(self):
        self._cache_scrapyd_log_file_info()
        data = {
            'database_id': self.database_id,
            'data_md5': self.get_md5_of_data_folder(),
            'data_size': self.get_size_of_data_folder(),
            'scrapy_log_file_found': (True if self._scrapy_log_file else False),
            # This could come out as 0 (no errors) or None (not known) - that's ok.
            'errors_count':
                (self._scrapy_log_file.get_errors_sent_to_process_count() if self._scrapy_log_file else None)
        }
        file_descriptor, filename = tempfile.mkstemp(prefix='archive', suffix='.json')
        with open(filename, 'w') as file:
            json.dump(data, file, indent=2)
        os.close(file_descriptor)
        return filename

    def get_md5_of_data_folder(self):
        if self._data_md5 is not None:
            return self._data_md5

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

        self._data_md5 = output.strip()
        return self._data_md5

    def get_size_of_data_folder(self):
        if self._data_size is not None:
            return self._data_size

        dir = self.config.directory_data + '/' + self.source_id + '/' + self.data_version.strftime("%Y%m%d_%H%M%S")
        args = ['du', '-sb', dir]

        output = subprocess.check_output(
            args,
            universal_newlines=True,
        )

        self._data_size = int(output.split('\t')[0])
        return self._data_size

    def get_data_files_exist(self):
        dir_name = self.config.directory_data + '/' + self.source_id + '/' + \
                   self.data_version.strftime("%Y%m%d_%H%M%S")

        # It must exist and be a directory
        return os.path.isdir(dir_name)

    def _cache_scrapyd_log_file_info(self):
        if self._scrapy_log_file_name is not None:
            return

        dir_to_search = os.path.join(self.config.directory_logs, self.source_id)
        if not os.path.isdir(dir_to_search):
            return
        for filename in os.listdir(dir_to_search):
            if filename.endswith(".log"):
                slf = ScrapyLogFile(os.path.join(dir_to_search, filename))
                if slf.does_match_date_version(self.data_version):
                    self._scrapy_log_file = slf
                    self._scrapy_log_file_name = os.path.join(dir_to_search, filename)
                    return

    def is_subset(self):
        self._cache_scrapyd_log_file_info()
        return self._scrapy_log_file and self._scrapy_log_file.is_subset()

    def has_errors_count(self):
        self._cache_scrapyd_log_file_info()
        return True if self._scrapy_log_file else False

    def get_errors_count(self):
        self._cache_scrapyd_log_file_info()
        return self._scrapy_log_file.get_errors_sent_to_process_count() if self._scrapy_log_file else None

    def write_data_file(self):
        file_descriptor, filename = tempfile.mkstemp(prefix='archive', suffix='.tar')
        os.close(file_descriptor)

        things_to_add = [
            self.config.directory_data + '/' + self.source_id + '/' +
            self.data_version.strftime("%Y%m%d_%H%M%S")
        ]

        self._cache_scrapyd_log_file_info()
        if self._scrapy_log_file_name:
            things_to_add.append(self._scrapy_log_file_name)

        command1 = 'tar -cf ' + filename + ' ' + ' '.join(things_to_add)
        return1 = os.system(command1)
        if return1 != 0:
            raise Exception(command1 + ' Got Return ' + str(return1))

        command2 = 'lz4 ' + filename + ' ' + filename+'.lz4'
        return2 = os.system(command2)
        if return2 != 0:
            raise Exception(command2 + ' Got Return ' + str(return2))

        os.unlink(filename)

        return filename+'.lz4'

    def get_s3_directory(self):
        return self.source_id + '/' + str(self.data_version.year) + '/' + str(self.data_version.month).zfill(2)

    def delete_data_files(self):
        if self.get_data_files_exist():
            data_dir = self.config.directory_data + '/' + self.source_id + '/' + \
                self.data_version.strftime("%Y%m%d_%H%M%S")
            # We use os.system here so we know the exact command so we can set up sudo correctly
            return1 = os.system('sudo -u ocdskfs /bin/rm -rf '+data_dir)
            if return1 != 0:
                raise Exception('delete_data_files Got Return ' + str(return1))

    def delete_log_files(self):
        self._cache_scrapyd_log_file_info()
        if self._scrapy_log_file_name and os.path.isfile(self._scrapy_log_file_name):
            # We use os.system here so we know the exact command so we can set up sudo correctly
            return1 = os.system('sudo -u ocdskfs /bin/rm -f ' + self._scrapy_log_file_name)
            if return1 != 0:
                raise Exception('delete_log_files Got Return ' + str(return1))
            if os.path.isfile(self._scrapy_log_file_name + '.stats'):
                return2 = os.system('sudo -u ocdskfs /bin/rm -f ' + self._scrapy_log_file_name + '.stats')
                if return2 != 0:
                    raise Exception('delete_log_files (.stats file) Got Return ' + str(return2))
            self._scrapy_log_file_name = None
            self._scrapy_log_file = None
