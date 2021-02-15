import datetime
import json
import os
import tempfile
import time
from functools import partial

from xxhash import xxh3_128

from ocdskingfisherarchive.exceptions import FutureDataVersionError, SourceMismatchError
from ocdskingfisherarchive.scrapy_log_file import ScrapyLogFile
from ocdskingfisherarchive.tarfile import LZ4TarFile

DATA_VERSION_FORMAT = '%Y%m%d_%H%M%S'


class Crawl:
    """
    A representation of a Kingfisher Collect crawl.

    Crawl information might be loaded from a :class:`local cache<ocdskingfisherarchive.cache.Cache>` or from
    :class:`remote storage<ocdskingfisherarchive.s3.S3>`, or constructed from scratch.
    """

    @classmethod
    def all(cls, data_directory, logs_directory):
        """
        Yields a :class:`~ocdskingfisherarchive.crawl.Crawl` instance for each non-sample crawl directory to which no
        files have been written in 7 days.

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
                    continue

                yield cls(source_id.name, parsed, data_directory=data_directory, logs_directory=logs_directory)

    @staticmethod
    def parse_data_version(string):
        """
        :param str string: a string in the format "YYMMDD_HHMMSS"
        :returns: a datetime if the format is correct, otherwise ``None``
        :rtype: datetime.datetime
        """
        try:
            return datetime.datetime.strptime(string, DATA_VERSION_FORMAT)
        except ValueError:
            pass

    def format_data_version(self):
        """
        :param datetime.datetime data_version: a datetime
        :returns: a string in the format "YYMMDD_HHMMSS"
        :rtype: str
        """
        return self.data_version.strftime(DATA_VERSION_FORMAT)

    def __init__(self, source_id, data_version, data_directory=None, logs_directory=None, **kwargs):
        """
        :param str data_directory: Kingfisher Collect's FILES_STORE directory
        :param str source_id: the spider's name
        :param str data_version: the crawl directory's name, parsed as a datetime
        :param str logs_directory: Kingfisher Collect's project directory within Scrapyd's logs_dir directory
        """
        self.data_directory = data_directory
        self.logs_directory = logs_directory

        kwargs.update({
            'source_id': source_id,
            'data_version': data_version,
        })
        kwargs.setdefault('archived')

        if isinstance(kwargs['data_version'], str):
            kwargs['data_version'] = self.parse_data_version(kwargs['data_version'])
        if isinstance(kwargs['archived'], int):
            kwargs['archived'] = bool(kwargs['archived'])

        self._values = kwargs
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
        return f'{self.source_id}/{self.format_data_version()}'

    @property
    def source_id(self):
        return self._values['source_id']

    @property
    def data_version(self):
        return self._values['data_version']

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
        return os.path.join(self.data_directory, self.source_id, self.format_data_version())

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
        if 'reject_reason' in self._values:
            return self._values['reject_reason']

        if not os.path.isdir(self.local_directory):
            self._values['reject_reason'] = 'no_data_directory'
        elif not next(os.scandir(self.local_directory), None):
            self._values['reject_reason'] = 'no_data_files'
        elif not self.scrapy_log_file:
            self._values['reject_reason'] = 'no_log_file'
        elif not self.scrapy_log_file.is_finished():
            self._values['reject_reason'] = 'not_finished'
        elif not self.scrapy_log_file.is_complete():
            self._values['reject_reason'] = 'not_complete'
        elif self.scrapy_log_file.error_rate > 0.5:
            self._values['reject_reason'] = 'not_clean_enough'
        else:
            self._values['reject_reason'] = None

        return self._values['reject_reason']

    @property
    def scrapy_log_file(self):
        if self._scrapy_log_file is None and self.logs_directory:
            self._scrapy_log_file = ScrapyLogFile.find(self.logs_directory, self.source_id, self.data_version)

        return self._scrapy_log_file

    @property
    def files_count(self):
        return self._values.setdefault('files_count',
            self.scrapy_log_file and self.scrapy_log_file.item_counts['File']
        )

    @property
    def errors_count(self):
        return self._values.setdefault('errors_count',
            self.scrapy_log_file and self.scrapy_log_file.item_counts['FileError']
        )

    @property
    def checksum(self):
        """
        Returns the checksum of all data in the crawl directory.

        To ensure a consistent checksum for a given directory, it processes sub-directories and files in alphabetical
        order. It uses the xxHash non-cryptographic hash function and reads files in chunks to limit use of memory.

        :returns: the checksum of all data in the crawl directory
        :rtype: str
        """
        if 'checksum' in self._values:
            return self._values['checksum']

        hasher = xxh3_128()
        for root, dirs, files in os.walk(self.local_directory):
            dirs.sort()
            for file in sorted(files):
                with open(os.path.join(root, file), 'rb') as f:
                    # xxsum reads 64KB at a time (https://github.com/Cyan4973/xxHash/blob/dev/xxhsum.c). If the end of
                    # a file could appear at the start of another file, we could add bytes for file boundaries.
                    for chunk in iter(partial(f.read, 65536), b''):  # 64KB
                        hasher.update(chunk)
        self._values['checksum'] = hasher.hexdigest()

        return self._values['checksum']

    @property
    def bytes(self):
        """
        :returns: the total size in bytes of all files in the crawl directory
        :rtype: int
        """
        if 'bytes' in self._values:
            return self._values['bytes']

        self._values['bytes'] = sum(os.path.getsize(os.path.join(root, file))
                                    for root, _, files in os.walk(self.local_directory) for file in files)

        return self._values['bytes']

    @property
    def archived(self):
        return self._values['archived']

    @archived.setter
    def archived(self, archived):
        self._values['archived'] = archived

    def asdict(self, cached=True):
        if cached:
            def getter(key):
                return self._values.get(key)
        else:
            def getter(key):
                return getattr(self, key)

        return {
            'id': self.pk,
            'source_id': self.source_id,
            'data_version': self.format_data_version(),
            'bytes': getter('bytes'),
            'checksum': getter('checksum'),
            'files_count': getter('files_count'),
            'errors_count': getter('errors_count'),
            'reject_reason': getter('reject_reason'),
            'archived': getter('archived'),
        }

    def compare(self, other):
        """
        Returns whether to archive this crawl in preference to another crawl.

        This implements Kingfisher Process' `data retention policy
        <https://ocdsdeploy.readthedocs.io/en/latest/use/kingfisher-process.html#data-retention-policy>`__.

        If there is an earlier crawl in the same month, this crawl is preferred to the earlier crawl if it has more
        bytes (and is thus distinct) and:

        -  has 50% more bytes
        -  has 50% more files
        -  is more clean

        If there is an earlier crawl in an earlier month, this crawl is not preferred if, compared to the most recent
        earlier crawl, it:

        -  is less clean and less complete (in which case it might have been identical, if not for the errors)
        -  is not distinct (the checksums are identical)

        Otherwise, it is preferred.

        :returns: whether this crawl is preferred for archival, and the reason
        :rtype: tuple
        """
        # Issue: https://github.com/open-contracting/deploy/issues/153#issuecomment-670186295

        if self.source_id != other.source_id:
            raise SourceMismatchError(f'Crawl source mismatch: {self.source_id} != {other.source_id}')
        # Crawls should only be compared in chronological order.
        if other.data_version.year > self.data_version.year or other.data_version.month > self.data_version.month:
            raise FutureDataVersionError(f'Future data version: {other.data_version} > {self.data_version}')

        # We run tests from least to most expensive, except where the logic requires otherwise:
        #
        # - Tests against Scrapy log files
        # - Counting bytes requires a ``stat()`` system call for each file
        # - Calculating checksums requires reading each file

        if other.data_version.year == self.data_version.year and other.data_version.month == self.data_version.month:
            if self.bytes > other.bytes:
                if self.bytes >= other.bytes * 1.5:
                    return True, 'same_period_more_bytes'
                if self.files_count >= other.files_count * 1.5:
                    return True, 'same_period_more_files'
                if self.errors_count < other.errors_count:
                    return True, 'same_period_more_clean'

            return False, 'same_period'

        if other.data_version.year < self.data_version.year or other.data_version.month < self.data_version.month:
            if (
                self.errors_count > other.errors_count
                and self.files_count <= other.files_count
                and self.bytes <= other.bytes
            ):
                return False, f'{other.data_version.year}_{other.data_version.month}_not_distinct_maybe'
            if other.checksum == self.checksum:
                return False, f'{other.data_version.year}_{other.data_version.month}_not_distinct'

        return True, 'new_period'

    def write_meta_data_file(self):
        file_descriptor, filename = tempfile.mkstemp(prefix='archive', suffix='.json')
        with open(filename, 'w') as f:
            json.dump(self.asdict(), f, indent=2)

        os.close(file_descriptor)
        return filename

    def write_data_file(self):
        file_descriptor, filename = tempfile.mkstemp(prefix='archive', suffix='.tar.lz4')
        with LZ4TarFile.open(filename, 'w:lz4') as tar:
            tar.add(self.local_directory)

        os.close(file_descriptor)
        return filename
