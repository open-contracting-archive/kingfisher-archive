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
        self._cached_scrapy_log_file_name = None
        self._cached_scrapy_log_file = None

    def _get_data_dir_name(self):
        return os.path.join(self.config.directory_data, self.source_id, self.data_version.strftime("%Y%m%d_%H%M%S"))

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

        cmd = 'find '+self._get_data_dir_name() + \
              ' -type f -exec md5sum {} + | awk \'{print $1}\' | sort | md5sum | awk \'{print $1}\''

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

        args = ['du', '-sb', self._get_data_dir_name()]

        output = subprocess.check_output(
            args,
            universal_newlines=True,
        )

        self._data_size = int(output.split('\t')[0])
        return self._data_size

    def get_data_files_exist(self):
        # It must exist and be a directory
        return os.path.isdir(self._get_data_dir_name())

    def _cache_scrapyd_log_file_info(self):
        if self._cached_scrapy_log_file_name is not None:
            return

        dir_to_search = os.path.join(self.config.directory_logs, self.source_id)
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

        command1 = f'tar -cf {filename} ' + ' '.join(things_to_add)
        return1 = os.system(command1)
        if return1 != 0:
            raise Exception(f'{command1} Got Return {return1}')

        command2 = f'lz4 {filename} {filename}.lz4'
        return2 = os.system(command2)
        if return2 != 0:
            raise Exception(f'{command2} Got Return {return2}')

        os.unlink(filename)

        return filename+'.lz4'

    def get_s3_directory(self):
        return '/'.join([self.source_id, str(self.data_version.year), str(self.data_version.month).zfill(2)])

    def delete_data_files(self):
        if self.get_data_files_exist():
            # We use os.system here so we know the exact command so we can set up sudo correctly
            return1 = os.system('sudo -u ocdskfs /bin/rm -rf '+self._get_data_dir_name())
            if return1 != 0:
                raise Exception(f'delete_data_files Got Return {return1}')

    def delete_log_files(self):
        if self.scrapy_log_file_name and os.path.isfile(self.scrapy_log_file_name):
            # We use os.system here so we know the exact command so we can set up sudo correctly
            return1 = os.system(f'sudo -u ocdskfs /bin/rm -f {self.scrapy_log_file_name}')
            if return1 != 0:
                raise Exception(f'delete_log_files Got Return {return1}')
            if os.path.isfile(f'{self.scrapy_log_file_name}.stats'):
                return2 = os.system(f'sudo -u ocdskfs /bin/rm -f {self.scrapy_log_file_name}.stats')
                if return2 != 0:
                    raise Exception(f'delete_log_files (.stats file) Got Return {return2}')
            self._cached_scrapy_log_file_name = None
            self._cached_scrapy_log_file = None
