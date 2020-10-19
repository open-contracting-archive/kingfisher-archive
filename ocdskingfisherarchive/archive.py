import logging
import os
import shutil

import dj_database_url
import psycopg2

from ocdskingfisherarchive.collection import Collection
from ocdskingfisherarchive.database_archive import DataBaseArchive
from ocdskingfisherarchive.s3 import S3

logger = logging.getLogger('ocdskingfisher.archive')


class Archive:
    def __init__(self, bucket_name, data_directory, logs_directory, database_file, database_url):
        self.s3 = S3(bucket_name)
        self.data_directory = data_directory
        self.logs_directory = logs_directory
        self.database_archive = DataBaseArchive(database_file)
        self.database_url = database_url

    def process(self, dry_run=False):
        collections = self.get_collections_to_consider_archiving()
        for collection in collections:
            self.process_collection(collection, dry_run)

    def get_collections_to_consider_archiving(self):
        connection = psycopg2.connect(dj_database_url.parse(self.database_url))
        cursor = connection.cursor()

        sql = "SELECT id, source_id, data_version  FROM collection " \
              "WHERE sample IS FALSE AND store_end_at IS NOT NULL AND transform_type = '' AND deleted_at IS NULL " \
              "ORDER BY id ASC"
        collections = []
        cursor.execute(sql)
        for row in cursor.fetchall():
            collections.append(Collection(row[0], row[1], row[2], self.data_directory, self.logs_directory))
        return collections

    def process_collection(self, collection, dry_run=False):
        # Check local database
        current_state = self.database_archive.get_state_of_crawl(collection)
        if current_state != 'UNKNOWN':
            logger.info('Ignoring %s; Local state is %s', collection, current_state)
            return

        # TODO If not archiving, still delete local files after 90 days

        # Work out what to do, archive if we should
        should_archive = self.should_we_archive_collection(collection)
        if dry_run:
            logger.info("Collection %s result: %s", collection, "Archive" if should_archive else "Skip")
            print("Collection %s result: %s", collection, "Archive" if should_archive else "Skip")
        elif should_archive:
            self.archive_collection(collection)
            self.database_archive.set_state_of_crawl(collection, 'ARCHIVED')
        else:
            self.database_archive.set_state_of_crawl(collection, 'DO NOT ARCHIVE')

    def should_we_archive_collection(self, collection):
        if not os.path.isdir(collection.directory):
            logger.info('Skipping %s because data files do not exist', collection)
            return False

        if not collection.scrapy_log_file:
            logger.info('Skipping %s because log file does not exist', collection)
            return False

        # Is it a subset; was from or until date set? (Sample is already checked but may as well check again)
        if collection.scrapy_log_file.is_subset():
            logger.info('Skipping %s because collection is a subset', collection)
            return False

        # If not finished, don't archive
        # (Note if loaded from Process database we check this there;
        #  but we may load from other places in the future so check again)
        if not collection.scrapy_log_file.is_finished():
            logger.info('Skipping %s because Scrapy log file says it is not finished', collection)
            return False

        # Is there already a collection archived for source / year / month?
        remote_metadata = self.s3.load_exact(collection.source_id, collection.data_version)
        if remote_metadata:
            # If checksums identical, leave it
            if remote_metadata['data_md5'] == collection.data_md5:
                logger.info('Skipping %s because an archive exists for same period and same MD5', collection)
                return False

            # If the local directory has more errors, leave it
            # (But we may not have an errors count for one of the things we are comparing)
            if remote_metadata['errors_count'] is not None and \
                    collection.scrapy_log_file.errors_count > remote_metadata['errors_count']:
                logger.info('Skipping %s because an archive exists for same period and fewer errors', collection)
                return False

            # If the local directory has equal or fewer bytes, leave it
            if collection.data_size <= remote_metadata['data_size']:
                logger.info('Skipping %s because an archive exists for same period and same or larger size',
                            collection)
                return False

            # Otherwise, Backup
            logger.info('Archiving %s because an archive exists for same period and we can not find a good reason to '
                        'not archive', collection)
            return True

        # Is an earlier collection archived for source?
        remote_metadata, year, month = self.s3.load_latest(collection.source_id, collection.data_version)
        if remote_metadata:
            # If checksums identical, leave it
            if remote_metadata['data_md5'] == collection.data_md5:
                logger.info('Skipping %s because an archive exists from earlier period (%s/%s) and same MD5',
                            collection, year, month)
                return False

            # Complete: If the local directory has 50% more bytes, replace the remote directory.
            if collection.data_size >= remote_metadata['data_size'] * 1.5:
                logger.info('Archiving %s because an archive exists from earlier period (%s/%s) and local collection '
                            'has 50%% more size', collection, year, month)
                return True

            # Clean: If the local directory has fewer or same errors, and greater or equal bytes,
            # replace the remote directory.
            # (But we may not have an errors count for one of the things we are comparing)
            if remote_metadata['errors_count'] is not None and \
                    collection.scrapy_log_file.errors_count <= remote_metadata['errors_count'] and \
                    collection.data_size >= remote_metadata['data_size']:
                logger.info('Archiving %s because an archive exists from earlier period (%s/%s) and local collection '
                            'has fewer or equal errors and greater or equal size', collection, year, month)
                return True

            # Otherwise, do not backup
            logger.info('Skipping %s because an archive exists from earlier period (%s/%s) and we can not find a '
                        'good reason to backup', collection, year, month)
            return False

        logger.info('Archiving %s because no current or previous archives found', collection)
        return True

    def archive_collection(self, collection):
        data_file_name = collection.write_data_file()
        meta_file_name = collection.write_meta_data_file()

        remote_directory = f'{collection.source_id}/{collection.data_version.year}/{collection.data_version.month:02d}'

        # Upload to staging
        self.s3.upload_file_to_staging(meta_file_name, f'{remote_directory}/metadata.json')
        self.s3.upload_file_to_staging(data_file_name, f'{remote_directory}/data.tar.lz4')

        # Move files in S3
        self.s3.move_file_from_staging_to_real(f'{remote_directory}/metadata.json')
        self.s3.move_file_from_staging_to_real(f'{remote_directory}/data.tar.lz4')

        # Delete staging files in S3
        self.s3.remove_staging_file(f'{remote_directory}/metadata.json')
        self.s3.remove_staging_file(f'{remote_directory}/data.tar.lz4')

        # Cleanup
        os.unlink(meta_file_name)
        os.unlink(data_file_name)
        shutil.rmtree(collection.directory)
        collection.scrapy_log_file.delete()

        logger.info('Archived %s', collection)
