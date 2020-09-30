import logging
import os
from ocdskingfisherarchive.archived_collection import ArchivedCollection


class Archive:

    def __init__(self, config, database_archive, database_process, s3):
        self.config = config
        self.database_archive = database_archive
        self.database_process = database_process
        self.s3 = s3
        self.logger = logging.getLogger('ocdskingfisher.archive')

    def process(self, dry_run=False):
        collections = self.database_process.get_collections_to_consider_archiving()
        for collection in collections:
            self.process_collection(collection, dry_run)

    def process_collection(self, collection, dry_run=False):
        self.logger.info("Processing collection " + str(collection.database_id))

        # Check local database
        current_state = self.database_archive.get_state_of_collection_id(collection.database_id)
        if current_state != 'UNKNOWN':
            self.logger.debug('Ignoring; Local state is ' + current_state)
            return

        # TODO If not archiving, still delete local files after 90 days

        # Work out what to do, archive if we should
        should_archive = self.should_we_archive_collection(collection)
        if dry_run:
            self.logger.info(
                "Collection " + str(collection.database_id) + " result: " + ("Archive" if should_archive else "Leave")
            )
            print(
                "Collection " + str(collection.database_id) + " result: " + ("Archive" if should_archive else "Leave")
            )
        elif should_archive:
            self.archive_collection(collection)
            self.database_archive.set_state_of_collection_id(collection.database_id, 'ARCHIVED')
        else:
            self.database_archive.set_state_of_collection_id(collection.database_id, 'DO NOT ARCHIVE')

    def should_we_archive_collection(self, collection):
        self.logger.info("Checking if we should archive collection " + str(collection.database_id))

        if not collection.get_data_files_exist():
            self.logger.debug('Not Archiving Cos data files do not exist')
            return False

        # Is it a subset; was from or until date set? (Sample is already checked but may as well check again)
        if collection.is_subset():
            self.logger.debug('Not Archiving Cos collection is a subset')
            return False

        # Is there already a collection archived for source / year / month?
        exact_archived_collection = self._get_exact_archived_collection(collection)
        if exact_archived_collection:
            self.logger.debug('Found an archive exists with same year/month')

            # If checksums identical, leave it
            if exact_archived_collection.get_data_md5() == collection.get_md5_of_data_folder():
                self.logger.debug('Not Archiving Cos An archive exists with same year/month and same MD5')
                return False

            # If the local directory has more errors, leave it
            # (But we may not have an errors count for one of the things we are comparing)
            if collection.has_errors_count() and exact_archived_collection.has_errors_count() and \
                    collection.get_errors_count() > exact_archived_collection.get_errors_count():
                self.logger.debug('Not Archiving Cos An archive exists with less errors')
                return False

            # If the local directory has equal or fewer bytes, leave it
            if collection.get_size_of_data_folder() <= exact_archived_collection.get_data_size():
                self.logger.debug('Not Archiving Cos An archive exists with same year/month and same or larger size')
                return False

            # Otherwise, Backup
            self.logger.debug(
                'Archiving Cos An archive exists with same year/month and we can not find a good reason to not archive'
            )
            return True

        # Is an earlier collection archived for source?
        last_archived_collection = self._get_last_archived_collection(collection)
        if last_archived_collection:
            self.logger.debug(
                'Found an archive exists with earlier year/month: {}/{}'.format(
                    last_archived_collection.year, last_archived_collection.month
                )
            )

            # If checksums identical, leave it
            if last_archived_collection.get_data_md5() == collection.get_md5_of_data_folder():
                self.logger.debug('Not Archiving Cos An archive exists with older year/month and same MD5')
                return False

            # Complete: If the local directory has 50% more bytes, replace the remote directory.
            if collection.get_size_of_data_folder() >= last_archived_collection.get_data_size() * 1.5:
                self.logger.debug('Archiving Cos An archive exists with older year/month and ' +
                                  'this collection has 50% more size')
                return True

            # Clean: If the local directory has fewer errors, and greater or equal bytes, replace the remote directory.
            # (But we may not have an errors count for one of the things we are comparing)
            if collection.has_errors_count() and last_archived_collection.has_errors_count() and \
                    collection.get_errors_count() < last_archived_collection.get_errors_count() and \
                    collection.get_size_of_data_folder() >= last_archived_collection.get_data_size():
                self.logger.debug('Archiving Cos An archive exists with older year/month and ' +
                                  'local collection has less errors and same or bigger size')
                return True

            # Otherwise, do not backup
            self.logger.debug('Not Archiving Cos An Older archive exists and we can not find a good reason to backup')
            return False

        self.logger.debug('Archiving - no current or previous archives found')
        return True

    def _get_exact_archived_collection(self, collection):
        # This is a separate method for mocking during testing
        return ArchivedCollection.load_exact(
            s3=self.s3,
            source_id=collection.source_id,
            data_version=collection.data_version
        )

    def _get_last_archived_collection(self, collection):
        # This is a separate method for mocking during testing
        return ArchivedCollection.load_latest(
            s3=self.s3,
            source_id=collection.source_id,
            data_version=collection.data_version
        )

    def archive_collection(self, collection):
        self.logger.info("Archiving collection " + str(collection.database_id))

        # Get Data file
        self.logger.debug('Getting Data File')
        data_file_name = collection.write_data_file()

        # Get Metadata file
        self.logger.debug('Getting MetaData File')
        meta_file_name = collection.write_meta_data_file()

        # Upload to staging
        self.logger.debug('Upload to Staging')
        s3_directory = collection.get_s3_directory()
        self.s3.upload_file_to_staging(meta_file_name, s3_directory + '/metadata.json')
        self.s3.upload_file_to_staging(data_file_name, s3_directory + '/data.tar.lz4')

        # Move files in S3
        self.logger.debug('Move files in S3')
        self.s3.move_file_from_staging_to_real(s3_directory + '/metadata.json')
        self.s3.move_file_from_staging_to_real(s3_directory + '/data.tar.lz4')

        # Delete Staging Files in S3
        self.logger.debug('Delete Staging files in S3')
        self.s3.remove_staging_file(s3_directory + '/metadata.json')
        self.s3.remove_staging_file(s3_directory + '/data.tar.lz4')

        # Delete local files we made
        self.logger.debug('Delete local files we made')
        os.unlink(meta_file_name)
        os.unlink(data_file_name)

        # Delete data files
        self.logger.debug('Delete data files')
        collection.delete_data_files()

        # Delete log files
        self.logger.debug('Delete log files')
        collection.delete_log_files()
