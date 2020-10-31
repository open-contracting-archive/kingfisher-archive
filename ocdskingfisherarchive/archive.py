import logging
import os
import shutil

from ocdskingfisherarchive.cache import Cache
from ocdskingfisherarchive.crawl import Crawl
from ocdskingfisherarchive.s3 import S3

logger = logging.getLogger('ocdskingfisher.archive')


class Archive:
    def __init__(self, bucket_name, data_directory, logs_directory, cache_file, cached_expired=False):
        """
        :param str bucket_name: an Amazon S3 bucket name
        :param str data_directory: Kingfisher Collect's FILES_STORE directory
        :param str logs_directory: Kingfisher Collect's project directory within Scrapyd's logs_dir directory
        :param str cache_file: the path to a SQLite database for caching the local state
        :param bool cached_expired: whether to ignore and overwrite existing rows in the SQLite database
        """
        self.s3 = S3(bucket_name)
        self.data_directory = data_directory
        self.logs_directory = logs_directory
        self.cache = Cache(cache_file, expired=cached_expired)

    def process(self, dry_run=False):
        """
        Runs the archival process.

        :param bool dry_run: whether to modify the filesystem and the bucket
        """
        for crawl in Crawl.all(self.data_directory, self.logs_directory):
            self.process_crawl(crawl, dry_run)

    def process_crawl(self, crawl, dry_run=False):
        """
        Runs the archival process for a single crawl.

        If the cache indicates that the crawl was already processed, the process ends. Otherwise, the crawl is archived
        if appropriate, and the cache is updated.

        :param ocdskingfisherarchive.crawl.Crawl crawl: a crawl
        :param bool dry_run: whether to modify the filesystem and the bucket
        """
        metadata, archived = self.cache.get(crawl)
        if archived is not None:
            logger.info('Ignoring %s: previously %s', crawl, 'archived' if archived else 'skipped')
            return

        # TODO If not archiving, still delete local files after 90 days

        should_archive, reason = self.should_archive(crawl)
        if should_archive:
            logger.info('ARCHIVE (%s) %s', reason, crawl)
        else:
            logger.info('skip (%s) %s', reason, crawl)
        if dry_run:
            return should_archive

        elif should_archive:
            self.archive_crawl(crawl)
            self.cache.set(crawl, True)
        else:
            self.cache.set(crawl, False)
        return should_archive

    def should_archive(self, crawl):
        """
        Returns whether to archive the crawl.

        This implements Kingfisher Process' `data retention policy
        <https://ocdsdeploy.readthedocs.io/en/latest/use/kingfisher-process.html#data-retention-policy>`__.

        The crawl will not be archived if it:

        -  has no data directory
        -  has no data files
        -  has no log file
        -  is not finished, according to the log
        -  is not complete, according to the log (it uses spider arguments to filter results)
        -  is insufficiently clean, according to the log (it has more error responses than success responses)

        If a crawl passes these tests, it is compared to archived crawls. If there is an earlier crawl in the same
        month, the new crawl will **replace** the earlier crawl if it has more bytes (and is thus distinct) and:

        -  has 50% more bytes
        -  has 50% more files
        -  is more clean

        If there is an earlier crawl in an earlier month, it will compare to the most recent, and the new crawl will
        not be archived if it:

        -  is less clean and less complete (in which case it might have been identical, if not for the errors)
        -  is not distinct (the checksums are identical)

        Otherwise, it will archive the crawl.

        :returns: whether to archive the crawl
        :rtype: bool
        """
        # Issue: https://github.com/open-contracting/deploy/issues/153#issuecomment-670186295

        if not os.path.isdir(crawl.directory):
            return False, 'no_data_directory'

        if not next(os.scandir(crawl.directory), None):
            return False, 'no_data_files'

        if not crawl.scrapy_log_file:
            return False, 'no_log_file'

        if not crawl.scrapy_log_file.is_finished():
            return False, 'not_finished'

        if not crawl.scrapy_log_file.is_complete():
            return False, 'not_complete'

        if crawl.scrapy_log_file.error_rate > 0.5:
            return False, 'not_clean_enough'

        # We run tests from least to most expensive, except where the logic requires otherwise:
        #
        # - Tests against Scrapy log files
        # - Counting bytes requires a ``stat()`` system call for each file
        # - Calculating checksums requires reading each file

        remote_metadata = self.s3.load_exact(crawl.source_id, crawl.data_version)
        if remote_metadata:
            if crawl.bytes > remote_metadata.bytes:
                if crawl.bytes >= remote_metadata.bytes * 1.5:
                    return True, 'same_period_more_bytes'
                if crawl.scrapy_log_file.item_counts['File'] >= remote_metadata.files_count * 1.5:
                    return True, 'same_period_more_files'
                if crawl.scrapy_log_file.item_counts['FileError'] < remote_metadata.errors_count:
                    return True, 'same_period_more_clean'

            return False, 'same_period'

        remote_metadata, year, month = self.s3.load_latest(crawl.source_id, crawl.data_version)
        if remote_metadata:
            if (
                crawl.scrapy_log_file.item_counts['FileError'] > remote_metadata.errors_count
                and crawl.scrapy_log_file.item_counts['File'] <= remote_metadata.files_count
                and crawl.bytes <= remote_metadata.bytes
            ):
                return False, f'{year}_{month}_not_distinct_maybe'

            if remote_metadata.checksum == crawl.checksum:
                return False, f'{year}_{month}_not_distinct'

        return True, 'new_period'

    def archive_crawl(self, crawl):
        """
        Performs the archival of the crawl.

        Creates data and metadata files, uploads them to the staging directory in the bucket, copies them to the final
        directory in the bucket, and deletes them in the staging directory.

        -  The final directory follows the pattern ``source_id/YY/MM``. As such, if a new crawl better meets the
           archival criteria than an old crawl in the same period, the old crawl's files are overwritten.
        -  The presence of a final directory indicates the crawl has already been archived. Therefore, we limit the
           risk of an incomplete upload using a staged process. (Leftover files indicate an incomplete upload.)

        Finally, it deletes the created files, the crawl's data directory, and the crawl's log file.
        """
        meta_file_name = crawl.write_meta_data_file()
        data_file_name = crawl.write_data_file()

        remote_directory = f'{crawl.source_id}/{crawl.data_version.year}/{crawl.data_version.month:02d}'

        files = {
            meta_file_name: f'{remote_directory}/metadata.json',
            data_file_name: f'{remote_directory}/data.tar.lz4',
            crawl.scrapy_log_file.name: f'{remote_directory}/scrapy.log',
        }

        for local, remote in files.items():
            self.s3.upload_file_to_staging(local, remote)
        for remote in files.values():
            self.s3.move_file_from_staging_to_real(remote)
        for remote in files.values():
            self.s3.remove_staging_file(remote)

        os.unlink(meta_file_name)
        os.unlink(data_file_name)
        shutil.rmtree(crawl.directory)
        crawl.scrapy_log_file.delete()

        logger.info('Archived %s', crawl)
