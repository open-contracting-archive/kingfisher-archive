import datetime
import hashlib
import json
import os
import subprocess
import tarfile
import tempfile
from functools import partial

from ocdskingfisherarchive.scrapy_log_file import ScrapyLogFile


class Crawl:
    """
    A representation of a Kingfisher Collect crawl.
    """

    @classmethod
    def all(cls, data_directory, logs_directory):
        """
        Yields a :class:`~ocdskingfisherarchive.crawl.Crawl` instance for each crawl directory.

        :param str data_directory: Kingfisher Collect's FILES_STORE directory
        :param str logs_directory: Kingfisher Collect's project directory within Scrapyd's logs_dir directory
        """
        for source_id in os.listdir(data_directory):
            spider_directory = os.path.join(data_directory, source_id)
            if os.path.isdir(spider_directory):
                for data_version in os.listdir(spider_directory):
                    data_version = cls.parse_data_version(data_version)
                    if data_version:
                        scrapy_log_file = ScrapyLogFile.find(logs_directory, source_id, data_version)
                        yield cls(data_directory, source_id, data_version, scrapy_log_file)

    @staticmethod
    def parse_data_version(directory):
        """
        :param str directory: a directory name in the format "YYMMDD_HHMMSS"
        :returns: a datetime if the format is correct, otherwise ``None``
        :rtype: datatime.datetime
        """
        try:
            return datetime.datetime.strptime(directory, '%Y%m%d_%H%M%S')
        except ValueError:
            pass

    def __init__(self, data_directory, source_id, data_version, scrapy_log_file):
        """
        :param str data_directory: Kingfisher Collect's FILES_STORE directory
        :param str source_id: the spider's name
        :param str data_version: the crawl directory's name, parsed as a datetime
        :param ocdskingfisherarchive.scrapy_log_file.ScrapyLogFile: the Scrapy log file
        """
        self.data_directory = data_directory
        self.source_id = source_id
        self.data_version = data_version
        self.scrapy_log_file = scrapy_log_file

        self._checksum = None
        self._bytes = None

    def __str__(self):
        """
        :returns: the path to the crawl directory relative to the data directory
        :rtype: str
        """
        return os.path.join(self.source_id, self.data_version.strftime('%Y%m%d_%H%M%S'))

    @property
    def directory(self):
        """
        :returns: the full path to the crawl directory
        :rtype: str
        """
        return os.path.join(self.data_directory, self.source_id, self.data_version.strftime('%Y%m%d_%H%M%S'))

    @property
    def checksum(self):
        """
        Returns the checksum of all data in the crawl directory.

        To ensure a consistent checksum for a given directory, it processes sub-directories and files in alphabetical
        order. It uses the BLAKE2 cryptographic hash function and reads files in chunks to limit use of memory.

        :returns: the checksum of all data in the crawl directory
        :rtype: str
        """
        if self._checksum is not None:
            return self._checksum

        h = hashlib.blake2b()
        for root, dirs, files in os.walk(self.directory):
            dirs.sort()
            for file in sorted(files):
                with open(os.path.join(root, file), 'rb') as f:
                    for chunk in iter(partial(f.read, 8192), b''):
                        h.update(chunk)
        self._checksum = h.hexdigest()

        return self._checksum

    @property
    def bytes(self):
        """
        :returns: the total size in bytes of all files in the crawl directory
        :rtype: int
        """
        if self._bytes is not None:
            return self._bytes

        self._bytes = sum(os.path.getsize(os.path.join(root, file))
                          for root, _, files in os.walk(self.directory) for file in files)

        return self._bytes

    def write_meta_data_file(self):
        data = {
            'checksum': self.checksum,
            'bytes': self.bytes,
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

        with tarfile.open(filename, 'w') as tar:
            tar.add(self.directory)

        subprocess.run(['lz4', '--content-size', filename, f'{filename}.lz4'], check=True)

        os.unlink(filename)

        return f'{filename}.lz4'
