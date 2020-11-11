import datetime
import json
import logging
import os
import tempfile
import time
from functools import partial

from xxhash import xxh3_128

from ocdskingfisherarchive.scrapy_log_file import ScrapyLogFile
from ocdskingfisherarchive.tarfile import LZ4TarFile

logger = logging.getLogger('ocdskingfisher.archive')

DATA_VERSION_FORMAT = '%Y%m%d_%H%M%S'


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
        seven_weeks_ago = time.time() - 604800  # 7 * 24 * 60 * 60

        for source_id in os.scandir(data_directory):
            if not source_id.is_dir():
                continue
            if source_id.name.endswith('_sample'):
                continue

            for data_version in os.scandir(source_id.path):
                if not data_version.is_dir():
                    continue
                parsed = cls.parse_data_version(data_version.name)
                if not parsed:
                    continue
                if data_version.stat().st_mtime >= seven_weeks_ago:
                    logger.info('wait (recent) %s/%s', source_id.name, data_version.name)
                    continue

                yield cls(data_directory, source_id.name, parsed, logs_directory)

    @staticmethod
    def parse_data_version(directory):
        """
        :param str directory: a directory name in the format "YYMMDD_HHMMSS"
        :returns: a datetime if the format is correct, otherwise ``None``
        :rtype: datatime.datetime
        """
        try:
            return datetime.datetime.strptime(directory, DATA_VERSION_FORMAT)
        except ValueError:
            pass

    def __init__(self, data_directory, source_id, data_version, logs_directory):
        """
        :param str data_directory: Kingfisher Collect's FILES_STORE directory
        :param str source_id: the spider's name
        :param str data_version: the crawl directory's name, parsed as a datetime
        :param str logs_directory: Kingfisher Collect's project directory within Scrapyd's logs_dir directory
        """
        self.data_directory = data_directory
        self.source_id = source_id
        self.data_version = data_version
        self.logs_directory = logs_directory

        self._checksum = None
        self._bytes = None
        self._scrapy_log_file = None

    def __str__(self):
        """
        :returns: the path to the crawl directory relative to the data directory
        :rtype: str
        """
        return '/'.join([self.source_id, self.data_version.strftime(DATA_VERSION_FORMAT)])

    @property
    def directory(self):
        """
        :returns: the full path to the crawl directory
        :rtype: str
        """
        return os.path.join(self.data_directory, self.source_id, self.data_version.strftime(DATA_VERSION_FORMAT))

    @property
    def scrapy_log_file(self):
        if self._scrapy_log_file is None:
            self._scrapy_log_file = ScrapyLogFile.find(self.logs_directory, self.source_id, self.data_version)

        return self._scrapy_log_file

    @property
    def files_count(self):
        return self.logs_directory and self.scrapy_log_file.item_counts['File']

    @property
    def errors_count(self):
        return self.logs_directory and self.scrapy_log_file.item_counts['FileError']

    @property
    def checksum(self):
        """
        Returns the checksum of all data in the crawl directory.

        To ensure a consistent checksum for a given directory, it processes sub-directories and files in alphabetical
        order. It uses the xxHash non-cryptographic hash function and reads files in chunks to limit use of memory.

        :returns: the checksum of all data in the crawl directory
        :rtype: str
        """
        if self._checksum is not None:
            return self._checksum

        hasher = xxh3_128()
        for root, dirs, files in os.walk(self.directory):
            dirs.sort()
            for file in sorted(files):
                with open(os.path.join(root, file), 'rb') as f:
                    # xxsum reads 64KB at a time (https://github.com/Cyan4973/xxHash/blob/dev/xxhsum.c). If it were
                    # possible for the end of a file to appear at the start of another file, we could add bytes for
                    # file boundaries.
                    for chunk in iter(partial(f.read, 65536), b''):  # 64KB
                        hasher.update(chunk)
        self._checksum = hasher.hexdigest()

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
            'version': '1',
            'source_id': self.source_id,
            'data_version': self.data_version.strftime(DATA_VERSION_FORMAT),
            'checksum': self.checksum,
            'bytes': self.bytes,
            'files_count': self.files_count,
            'errors_count': self.errors_count,
        }

        file_descriptor, filename = tempfile.mkstemp(prefix='archive', suffix='.json')
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)

        os.close(file_descriptor)
        return filename

    def write_data_file(self):
        file_descriptor, filename = tempfile.mkstemp(prefix='archive', suffix='.tar.lz4')
        with LZ4TarFile.open(filename, 'w:lz4') as tar:
            tar.add(self.directory)

        os.close(file_descriptor)
        return filename
