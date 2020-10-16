import logging
import tempfile
from contextlib import contextmanager

import boto3
from botocore.exceptions import ClientError


@contextmanager
def _try(s3):
    try:
        yield
    except ClientError as e:
        s3.logger.error(e)
        raise e


class S3:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.logger = logging.getLogger('ocdskingfisher.archive')
        self.s3_client = boto3.client('s3')

    def upload_file_to_staging(self, local_file_name, remote_file_name):
        with _try(self):
            self.s3_client.upload_file(local_file_name, self.bucket_name, f'staging/{remote_file_name}')

    def move_file_from_staging_to_real(self, remote_file_name):
        copy_source = {
            'Bucket': self.bucket_name,
            'Key': f'staging/{remote_file_name}',
        }
        with _try(self):
            self.s3_client.copy(copy_source, self.bucket_name, remote_file_name)

    def remove_staging_file(self, remote_file_name):
        with _try(self):
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=f'staging/{remote_file_name}')

    def get_file(self, remote_file_name):
        try:
            with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as file:
                self.s3_client.download_fileobj(self.bucket_name, remote_file_name, file)
                return file.name
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                return None
            else:
                self.logger.error(e)
                raise e

    def get_years_and_months_for_source(self, source_id):
        with _try(self):
            # This is max 1000 responses but given how many files we should have per source this should be fine
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=f'{source_id}/')
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
