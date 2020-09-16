import tempfile

import boto3
from botocore.exceptions import ClientError


class S3:

    def __init__(self, config):
        self.config = config
        self.s3_client = boto3.client('s3')

    def upload_file_to_staging(self, local_file_name, remote_file_name):
        try:
            self.s3_client.upload_file(
                local_file_name,
                self.config.s3_bucket_name,
                'staging/' + remote_file_name
            )
        except ClientError as e:
            self.logger.error(e)
            raise e

    def move_file_from_staging_to_real(self, remote_file_name):
        try:
            self.s3_client.copy(
                {
                    'Bucket': self.config.s3_bucket_name,
                    'Key': 'staging/' + remote_file_name
                },
                self.config.s3_bucket_name,
                remote_file_name
            )
        except ClientError as e:
            self.logger.error(e)
            raise e

    def remove_staging_file(self, remote_file_name):
        try:
            self.s3_client.delete_object(
                Bucket=self.config.s3_bucket_name,
                Key='staging/' + remote_file_name
            )
        except ClientError as e:
            self.logger.error(e)
            raise e

    def get_file(self, remote_file_name):
        try:
            with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as file:
                self.s3_client.download_fileobj(self.config.s3_bucket_name, remote_file_name, file)
                return file.name
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                return None
            else:
                self.logger.error(e)
                raise e

    def get_years_and_months_for_source(self, source_id):
        try:
            # This is max 1000 responses but given how many files we should have per source this should be fine
            response = self.s3_client.list_objects_v2(
                Bucket=self.config.s3_bucket_name,
                Prefix=source_id+'/',
            )
            if response['KeyCount'] == 0:
                return {}
            out = {}
            for c in response['Contents']:
                key_bits = c['Key'].split('/')
                year = int(key_bits[1])
                month = int(key_bits[2])
                if year not in out:
                    out[year] = {}
                out[year][month] = True
            return out
        except ClientError as e:
            self.logger.error(e)
            raise e
