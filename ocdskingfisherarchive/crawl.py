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

                yield cls(source_id.name, parsed, data_directory=data_directory, logs_directory=logs_directory)

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

    def __init__(self, source_id, data_version, data_directory=None, logs_directory=None, cache=None):
        """
        :param str data_directory: Kingfisher Collect's FILES_STORE directory
        :param str source_id: the spider's name
        :param str data_version: the crawl directory's name, parsed as a datetime
        :param str logs_directory: Kingfisher Collect's project directory within Scrapyd's logs_dir directory
        """
        self.source_id = source_id
        if isinstance(data_version, str):
            self.data_version = self.parse_data_version(data_version)
        else:
            self.data_version = data_version
        self.data_directory = data_directory
        self.logs_directory = logs_directory

        if cache:
            self._cache = cache
        else:
            self._cache = {}
        self._scrapy_log_file = None

    def __str__(self):
        """
        :returns: the path to the crawl directory relative to the data directory
        :rtype: str
        """
        return self.pk

    @property
    def pk(self):
        """
        :returns: the path to the crawl directory relative to the data directory
        :rtype: str
        """
        return '/'.join([self.source_id, self.data_version.strftime(DATA_VERSION_FORMAT)])

    @property
    def remote_directory(self):
        """
        :returns: the path of the remote directory
        :rtype: str
        """
        return f'{self.source_id}/{self.data_version.year}/{self.data_version.month:02d}'

    @property
    def local_directory(self):
        """
        :returns: the full path to the crawl directory
        :rtype: str
        """
        return os.path.join(self.data_directory, self.source_id, self.data_version.strftime(DATA_VERSION_FORMAT))

    @property
    def reject_reason(self):
        """
        A crawl is not archived if it:

        -  has no data directory
        -  has no data files
        -  has no log file
        -  is not finished, according to the log
        -  is not complete, according to the log (it uses spider arguments to filter results)
        -  is insufficiently clean, according to the log (it has more error responses than success responses)

        :returns: the reason the crawl is not archivable, if any
        :rtype: str
        """
        if 'reject_reason' in self._cache:
            return self._cache['reject_reason']

        if not os.path.isdir(self.local_directory):
            self._cache['reject_reason'] = 'no_data_directory'
        elif not next(os.scandir(self.local_directory), None):
            self._cache['reject_reason'] = 'no_data_files'
        elif not self.scrapy_log_file:
            self._cache['reject_reason'] = 'no_log_file'
        elif not self.scrapy_log_file.is_finished():
            self._cache['reject_reason'] = 'not_finished'
        elif not self.scrapy_log_file.is_complete():
            self._cache['reject_reason'] = 'not_complete'
        elif self.scrapy_log_file.error_rate > 0.5:
            self._cache['reject_reason'] = 'not_clean_enough'
        else:
            self._cache['reject_reason'] = None

        return self._cache['reject_reason']

    @property
    def scrapy_log_file(self):
        if self._scrapy_log_file is None and self.logs_directory:
            self._scrapy_log_file = ScrapyLogFile.find(self.logs_directory, self.source_id, self.data_version)

        return self._scrapy_log_file

    @property
    def files_count(self):
        if 'files_count' not in self._cache:
            self._cache['files_count'] = self.scrapy_log_file and self.scrapy_log_file.item_counts['File']

        return self._cache['files_count']

    @property
    def errors_count(self):
        if 'errors_count' not in self._cache:
            self._cache['errors_count'] = self.scrapy_log_file and self.scrapy_log_file.item_counts['FileError']

        return self._cache['errors_count']

    @property
    def checksum(self):
        """
        Returns the checksum of all data in the crawl directory.

        To ensure a consistent checksum for a given directory, it processes sub-directories and files in alphabetical
        order. It uses the xxHash non-cryptographic hash function and reads files in chunks to limit use of memory.

        :returns: the checksum of all data in the crawl directory
        :rtype: str
        """
        if 'checksum' in self._cache:
            return self._cache['checksum']

        hasher = xxh3_128()
        for root, dirs, files in os.walk(self.local_directory):
            dirs.sort()
            for file in sorted(files):
                with open(os.path.join(root, file), 'rb') as f:
                    # xxsum reads 64KB at a time (https://github.com/Cyan4973/xxHash/blob/dev/xxhsum.c). If it were
                    # possible for the end of a file to appear at the start of another file, we could add bytes for
                    # file boundaries.
                    for chunk in iter(partial(f.read, 65536), b''):  # 64KB
                        hasher.update(chunk)
        self._cache['checksum'] = hasher.hexdigest()

        return self._cache['checksum']

    @property
    def bytes(self):
        """
        :returns: the total size in bytes of all files in the crawl directory
        :rtype: int
        """
        if 'bytes' in self._cache:
            return self._cache['bytes']

        self._cache['bytes'] = sum(os.path.getsize(os.path.join(root, file))
                                   for root, _, files in os.walk(self.local_directory) for file in files)

        return self._cache['bytes']

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
            tar.add(self.local_directory)

        os.close(file_descriptor)
        return filename
