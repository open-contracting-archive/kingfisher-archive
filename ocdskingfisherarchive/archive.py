import logging
import os
import shutil

from ocdskingfisherarchive.crawl import Crawl
from ocdskingfisherarchive.database import Database
from ocdskingfisherarchive.s3 import S3

logger = logging.getLogger('ocdskingfisher.archive')


class Archive:
    def __init__(self, bucket_name, data_directory, logs_directory, database_file):
        self.s3 = S3(bucket_name)
        self.data_directory = data_directory
        self.logs_directory = logs_directory
        self.database = Database(database_file)

    def process(self, dry_run=False):
        for crawl in Crawl.all(self.data_directory, self.logs_directory):
            self.process_crawl(crawl, dry_run)

    def process_crawl(self, crawl, dry_run=False):
        current_state = self.database.get_state(crawl)
        if current_state:
            logger.info('Ignoring %s; Local state is %s', crawl, current_state)
            return

        # TODO If not archiving, still delete local files after 90 days

        should_archive = self.should_we_archive_crawl(crawl)
        if dry_run:
            return

        elif should_archive:
            self.archive_crawl(crawl)
            self.database.set_state(crawl, 'ARCHIVED')
        else:
            self.database.set_state(crawl, 'SKIPPED')

    def should_we_archive_crawl(self, crawl):
        if not os.path.isdir(crawl.directory):
            logger.info('Skipping %s because data files do not exist', crawl)
            return False

        if not crawl.scrapy_log_file:
            logger.info('Skipping %s because log file does not exist', crawl)
            return False

        # Is it a subset; was from or until date set? (Sample is already checked but may as well check again)
        if crawl.scrapy_log_file.is_subset():
            logger.info('Skipping %s because crawl is a subset', crawl)
            return False

        # If not finished, don't archive
        # (Note if loaded from Process database we check this there;
        #  but we may load from other places in the future so check again)
        if not crawl.scrapy_log_file.is_finished():
            logger.info('Skipping %s because Scrapy log file says it is not finished', crawl)
            return False

        # Is there already a crawl archived for source / year / month?
        remote_metadata = self.s3.load_exact(crawl.source_id, crawl.data_version)
        if remote_metadata:
            # If checksums identical, leave it
            if remote_metadata['data_md5'] == crawl.data_md5:
                logger.info('Skipping %s because an archive exists for same period and same MD5', crawl)
                return False

            # If the local directory has more errors, leave it
            # (But we may not have an errors count for one of the things we are comparing)
            if remote_metadata['errors_count'] is not None and \
                    crawl.scrapy_log_file.errors_count > remote_metadata['errors_count']:
                logger.info('Skipping %s because an archive exists for same period and fewer errors', crawl)
                return False

            # If the local directory has equal or fewer bytes, leave it
            if crawl.data_size <= remote_metadata['data_size']:
                logger.info('Skipping %s because an archive exists for same period and same or larger size', crawl)
                return False

            # Otherwise, Backup
            logger.info('Archiving %s because an archive exists for same period and we can not find a good reason to '
                        'not archive', crawl)
            return True

        # Is an earlier crawl archived for source?
        remote_metadata, year, month = self.s3.load_latest(crawl.source_id, crawl.data_version)
        if remote_metadata:
            # If checksums identical, leave it
            if remote_metadata['data_md5'] == crawl.data_md5:
                logger.info('Skipping %s because an archive exists from earlier period (%s/%s) and same MD5',
                            crawl, year, month)
                return False

            # Complete: If the local directory has 50% more bytes, replace the remote directory.
            if crawl.data_size >= remote_metadata['data_size'] * 1.5:
                logger.info('Archiving %s because an archive exists from earlier period (%s/%s) and local crawl has '
                            '50%% more size', crawl, year, month)
                return True

            # Clean: If the local directory has fewer or same errors, and greater or equal bytes,
            # replace the remote directory.
            # (But we may not have an errors count for one of the things we are comparing)
            if remote_metadata['errors_count'] is not None and \
                    crawl.scrapy_log_file.errors_count <= remote_metadata['errors_count'] and \
                    crawl.data_size >= remote_metadata['data_size']:
                logger.info('Archiving %s because an archive exists from earlier period (%s/%s) and local crawl '
                            'has fewer or equal errors and greater or equal size', crawl, year, month)
                return True

            # Otherwise, do not backup
            logger.info('Skipping %s because an archive exists from earlier period (%s/%s) and we can not find a '
                        'good reason to backup', crawl, year, month)
            return False

        logger.info('Archiving %s because no current or previous archives found', crawl)
        return True

    def archive_crawl(self, crawl):
        data_file_name = crawl.write_data_file()
        meta_file_name = crawl.write_meta_data_file()

        remote_directory = f'{crawl.source_id}/{crawl.data_version.year}/{crawl.data_version.month:02d}'

        self.s3.upload_file_to_staging(meta_file_name, f'{remote_directory}/metadata.json')
        self.s3.upload_file_to_staging(data_file_name, f'{remote_directory}/data.tar.lz4')

        self.s3.move_file_from_staging_to_real(f'{remote_directory}/metadata.json')
        self.s3.move_file_from_staging_to_real(f'{remote_directory}/data.tar.lz4')

        self.s3.remove_staging_file(f'{remote_directory}/metadata.json')
        self.s3.remove_staging_file(f'{remote_directory}/data.tar.lz4')

        os.unlink(meta_file_name)
        os.unlink(data_file_name)
        shutil.rmtree(crawl.directory)
        crawl.scrapy_log_file.delete()

        logger.info('Archived %s', crawl)
