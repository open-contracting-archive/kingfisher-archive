import os

from botocore.exceptions import ClientError
from botocore.stub import Stubber

import ocdskingfisherarchive.s3
from tests import create_crawl_directory


def test_process_crawl(archiver, tmpdir, caplog, monkeypatch):
    def download_fileobj(*args, **kwargs):
        raise ClientError(error_response={'Error': {'Code': '404'}}, operation_name='')

    def list_objects_v2(*args, **kwargs):
        return {'KeyCount': 0}

    create_crawl_directory(tmpdir, ['data.json'], 'log_error1.log')
    os.utime(tmpdir.join('data', 'scotland', '20200902_052458'), (1, 1))

    stubber = Stubber(ocdskingfisherarchive.s3.client)
    monkeypatch.setattr(ocdskingfisherarchive.s3, 'client', stubber)
    # See https://github.com/boto/botocore/issues/974
    for method in ('upload_file', 'copy', 'delete_object'):
        monkeypatch.setattr(stubber, method, lambda *args, **kwargs: None, raising=False)
    monkeypatch.setattr(stubber, 'download_fileobj', download_fileobj, raising=False)
    monkeypatch.setattr(stubber, 'list_objects_v2', list_objects_v2, raising=False)
    stubber.activate()

    archiver.run()

    stubber.assert_no_pending_responses()

    directories = set()
    filenames = set()
    for root, dirs, files in os.walk(tmpdir):
        root_directory = root[len(str(tmpdir)) + 1:]
        for filename in files:
            filenames.add(os.path.join(root_directory, filename))
        for directory in dirs:
            directories.add(os.path.join(root_directory, directory))

    assert filenames == {'cache.sqlite3'}
    assert directories == {'data', os.path.join('data', 'scotland'), 'logs', os.path.join('logs', 'kingfisher'),
                           os.path.join('logs', 'kingfisher', 'scotland')}
