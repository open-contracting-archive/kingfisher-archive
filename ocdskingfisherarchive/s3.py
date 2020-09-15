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
