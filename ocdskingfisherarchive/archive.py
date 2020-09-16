import logging


class Archive:

    def __init__(self, config, database_archive, database_process, s3):
        self.config = config
        self.database_archive = database_archive
        self.database_process = database_process
        self.s3 = s3
        self.logger = logging.getLogger('ocdskingfisher.archive')

    def process(self):
        collections = self.database_process.get_collections_to_consider_archiving()
        for collection in collections:
            self.process_collection(collection)

    def process_collection(self, collection):
        self.logger.info("Processing collection " + str(collection.database_id))

        # Check local database
        current_state = self.database_archive.get_state_of_collection_id(collection.database_id)
        if current_state != 'UNKNOWN':
            return

        # Work out what to do, archive if we should
        new_state = self.should_we_archive_collection(collection)
        if new_state:
            self.archive_collection(collection)
            self.database_archive.set_state_of_collection_id(collection.database_id, 'ARCHIVED')
        else:
            self.database_archive.set_state_of_collection_id(collection.database_id, 'DO NOT ARCHIVE')

    def should_we_archive_collection(self, collection):
        self.logger.info("Checking if we should archive collection " + str(collection.database_id))
        # TODO check all criteria for should archive here
        return True

    def archive_collection(self, collection):
        self.logger.info("Archiving collection " + str(collection.database_id))

        # Get Data file
        data_file_name = collection.write_data_file()

        # Get Metadata file
        meta_file_name = collection.write_meta_data_file()

        # Upload to staging
        s3_directory = collection.get_s3_directory()
        self.s3.upload_file_to_staging(meta_file_name, s3_directory + '/metadata.json')
        self.s3.upload_file_to_staging(data_file_name, s3_directory + '/data.tar.lz4')

        # Move files in S3
        self.s3.move_file_from_staging_to_real(s3_directory + '/metadata.json')
        self.s3.move_file_from_staging_to_real(s3_directory + '/data.tar.lz4')

        # Delete Staging Files in S3
        self.s3.remove_staging_file(s3_directory + '/metadata.json')
        self.s3.remove_staging_file(s3_directory + '/data.tar.lz4')
