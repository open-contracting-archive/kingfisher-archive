import logging

import boto3
from botocore.exceptions import ClientError


class Archive:

    def __init__(self, config, database_archive, database_process):
        self.config = config
        self.database_archive = database_archive
        self.database_process = database_process
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
        s3_client = boto3.client('s3')
        try:
            s3_client.upload_file(
                meta_file_name,
                self.config.s3_bucket_name,
                'staging/' + s3_directory + '/metadata.json'
            )
        except ClientError as e:
            self.logger.error(e)
        try:
            s3_client.upload_file(
                data_file_name,
                self.config.s3_bucket_name,
                'staging/' + s3_directory + '/data.tar.lz'
            )
        except ClientError as e:
            self.logger.error(e)

        # Move files in S3
        try:
            s3_client.copy(
                {
                    'Bucket': self.config.s3_bucket_name,
                    'Key': 'staging/' + s3_directory + '/metadata.json'
                },
                self.config.s3_bucket_name,
                s3_directory + '/metadata.json'
            )
        except ClientError as e:
            self.logger.error(e)
        try:
            s3_client.copy(
                {
                    'Bucket': self.config.s3_bucket_name,
                    'Key': 'staging/' + s3_directory + '/data.tar.lz'
                },
                self.config.s3_bucket_name,
                s3_directory + '/data.tar.lz'
            )
        except ClientError as e:
            self.logger.error(e)

        # Delete Staging Files in S3
        try:
            s3_client.delete_object(
                Bucket=self.config.s3_bucket_name,
                Key='staging/' + s3_directory + '/metadata.json'
            )
        except ClientError as e:
            self.logger.error(e)
        try:
            s3_client.delete_object(
                Bucket=self.config.s3_bucket_name,
                Key='staging/' + s3_directory + '/data.tar.lz'
            )
        except ClientError as e:
            self.logger.error(e)
