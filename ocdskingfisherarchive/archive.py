import logging
import os

import dj_database_url
import psycopg2

from ocdskingfisherarchive.archived_collection import ArchivedCollection
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
        current_state = self.database_archive.get_state_of_collection_id(collection.database_id)
        if current_state != 'UNKNOWN':
            logger.debug('Ignoring %s; Local state is %s', collection.database_id, current_state)
            return

        # TODO If not archiving, still delete local files after 90 days

        # Work out what to do, archive if we should
        should_archive = self.should_we_archive_collection(collection)
        if dry_run:
            logger.info("Collection %s result: %s", collection.database_id, "Archive" if should_archive else "Skip")
            print("Collection %s result: %s", collection.database_id, "Archive" if should_archive else "Skip")
        elif should_archive:
            self.archive_collection(collection)
            self.database_archive.set_state_of_collection_id(collection.database_id, 'ARCHIVED')
        else:
            self.database_archive.set_state_of_collection_id(collection.database_id, 'DO NOT ARCHIVE')

    def should_we_archive_collection(self, collection):
        if not collection.get_data_files_exist():
            logger.debug('Skipping %s because data files do not exist', collection.database_id)
            return False

        # Is it a subset; was from or until date set? (Sample is already checked but may as well check again)
        if collection.is_subset():
            logger.debug('Skipping %s because collection is a subset', collection.database_id)
            return False

        # If not finished, don't archive
        # (Note if loaded from Process database we check this there;
        #  but we may load from other places in the future so check again)
        if collection.scrapy_log_file and not collection.scrapy_log_file.is_finished():
            logger.debug('Skipping %s because Scrapy log file says it is not finished', collection.database_id)
            return False

        # Is there already a collection archived for source / year / month?
        exact_archived_collection = self._get_exact_archived_collection(collection)
        if exact_archived_collection:
            # If checksums identical, leave it
            if exact_archived_collection.get_data_md5() == collection.get_md5_of_data_folder():
                logger.debug('Skipping %s because an archive exists for same period and same MD5',
                             collection.database_id)
                return False

            # If the local directory has more errors, leave it
            # (But we may not have an errors count for one of the things we are comparing)
            if collection.has_errors_count() and exact_archived_collection.has_errors_count() and \
                    collection.get_errors_count() > exact_archived_collection.get_errors_count():
                logger.debug('Skipping %s because an archive exists for same period and fewer errors',
                             collection.database_id)
                return False

            # If the local directory has equal or fewer bytes, leave it
            if collection.get_size_of_data_folder() <= exact_archived_collection.get_data_size():
                logger.debug('Skipping %s because an archive exists for same period and same or larger size',
                             collection.database_id)
                return False

            # Otherwise, Backup
            logger.info('Archiving %s because an archive exists for same period and we can not find a good reason to '
                        'not archive', collection.database_id)
            return True

        # Is an earlier collection archived for source?
        last = self._get_last_archived_collection(collection)
        if last:
            # If checksums identical, leave it
            if last.get_data_md5() == collection.get_md5_of_data_folder():
                logger.debug('Skipping %s because an archive exists from earlier period (%s/%s) and same MD5',
                             collection.database_id, last.year, last.month)
                return False

            # Complete: If the local directory has 50% more bytes, replace the remote directory.
            if collection.get_size_of_data_folder() >= last.get_data_size() * 1.5:
                logger.info('Archiving %s because an archive exists from earlier period (%s/%s) and local collection '
                            'has 50%% more size', collection.database_id, last.year, last.month)
                return True

            # Clean: If the local directory has fewer or same errors, and greater or equal bytes,
            # replace the remote directory.
            # (But we may not have an errors count for one of the things we are comparing)
            if collection.has_errors_count() and last.has_errors_count() and \
                    collection.get_errors_count() <= last.get_errors_count() and \
                    collection.get_size_of_data_folder() >= last.get_data_size():
                logger.info('Archiving %s because an archive exists from earlier period (%s/%s) and local collection '
                            'has fewer or equal errors and greater or equal size', collection.database_id, last.year,
                            last.month)
                return True

            # Otherwise, do not backup
            logger.debug('Skipping %s because an archive exists from earlier period (%s/%s) and we can not find a '
                         'good reason to backup', collection.database_id, last.year, last.month)
            return False

        logger.info('Archiving %s because no current or previous archives found', collection.database_id)
        return True

    def _get_exact_archived_collection(self, collection):
        return ArchivedCollection.load_exact(self.s3, collection.source_id, collection.data_version)

    def _get_last_archived_collection(self, collection):
        return ArchivedCollection.load_latest(self.s3, collection.source_id, collection.data_version)

    def archive_collection(self, collection):
        # Get data file
        data_file_name = collection.write_data_file()

        # Get metadata file
        meta_file_name = collection.write_meta_data_file()

        # Upload to staging
        s3_directory = collection.get_s3_directory()
        self.s3.upload_file_to_staging(meta_file_name, f'{s3_directory}/metadata.json')
        self.s3.upload_file_to_staging(data_file_name, f'{s3_directory}/data.tar.lz4')

        # Move files in S3
        self.s3.move_file_from_staging_to_real(f'{s3_directory}/metadata.json')
        self.s3.move_file_from_staging_to_real(f'{s3_directory}/data.tar.lz4')

        # Delete staging files in S3
        self.s3.remove_staging_file(f'{s3_directory}/metadata.json')
        self.s3.remove_staging_file(f'{s3_directory}/data.tar.lz4')

        # Delete local files we made
        os.unlink(meta_file_name)
        os.unlink(data_file_name)

        # Delete data files
        collection.delete_data_files()

        # Delete log files
        collection.delete_log_files()
        logger.info('Archived %s', collection.database_id)
