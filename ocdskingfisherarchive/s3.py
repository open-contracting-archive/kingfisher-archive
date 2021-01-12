import json
import logging
import os
import tempfile
from contextlib import contextmanager

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from ocdskingfisherarchive.crawl import Crawl

load_dotenv()
client = boto3.client('s3')
logger = logging.getLogger('ocdskingfisher.archive')


def _find_latest_year_month_to_load(data, year, month):
    while year >= 2018:
        if data.get(year, {}).get(month):
            return year, month
        if month > 1:
            month = month - 1
        else:
            year = year - 1
            month = 12
    return None, None


@contextmanager
def _try(s3):
    try:
        yield
    except ClientError as e:
        logger.error(e)
        raise e


class S3:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def load_exact(self, source_id, data_version):
        """
        Loads an archive from S3 for source && exact year/month, if it exists.
        """
        return self._load(source_id, data_version.year, data_version.month)

    def load_latest(self, source_id, data_version):
        """
        Loads an archive from S3 for source && the latest year/month up to the one passed, if any exist.
        """
        data = self.get_years_and_months_for_source(source_id)
        year, month = _find_latest_year_month_to_load(data, data_version.year, data_version.month)
        if year and month:
            return self._load(source_id, year, month)

    def _load(self, source_id, year, month):
        remote_filename = f'{source_id}/{year}/{month:02d}/metadata.json'
        filename = self.get_file(remote_filename)
        if filename:
            with open(filename) as f:
                metadata = json.load(f)
                crawl = Crawl(**metadata)
            os.unlink(filename)
            return crawl

    def upload_file_to_staging(self, local_file_name, remote_file_name):
        with _try(self):
            client.upload_file(local_file_name, self.bucket_name, f'staging/{remote_file_name}')

    def move_file_from_staging_to_real(self, remote_file_name):
        copy_source = {
            'Bucket': self.bucket_name,
            'Key': f'staging/{remote_file_name}',
        }
        with _try(self):
            client.copy(copy_source, self.bucket_name, remote_file_name, ExtraArgs={
                'MetadataDirective': 'COPY',
                'StorageClass': 'STANDARD_IA',
            })

    def remove_staging_file(self, remote_file_name):
        with _try(self):
            client.delete_object(Bucket=self.bucket_name, Key=f'staging/{remote_file_name}')

    def get_file(self, remote_file_name):
        try:
            with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as file:
                client.download_fileobj(self.bucket_name, remote_file_name, file)
                return file.name
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                return None
            else:
                logger.error(e)
                raise e

    def get_years_and_months_for_source(self, source_id):
        with _try(self):
            # This is max 1000 responses but given how many files we should have per source this should be fine
            response = client.list_objects_v2(Bucket=self.bucket_name, Prefix=f'{source_id}/')
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
