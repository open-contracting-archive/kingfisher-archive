import datetime
import os.path

import pytest

from ocdskingfisherarchive.crawl import Crawl
from tests import create_crawl_directory

data_version = datetime.datetime(2020, 9, 2, 5, 24, 58)


def test_all(tmpdir, caplog, monkeypatch):
    create_crawl_directory(tmpdir, ['data.json'], 'log_error1.log')

    crawls = list(Crawl.all(tmpdir.join('data'), tmpdir.join('logs', 'kingfisher')))

    assert len(crawls) == 1
    assert crawls[0].source_id == 'scotland'
    assert crawls[0].data_version == data_version
    assert crawls[0].data_directory == tmpdir.join('data')
    assert crawls[0].scrapy_log_file.name == tmpdir.join('logs', 'kingfisher', 'scotland',
                                                         '307e8331edc801c691e21690db130256.log')


def test_all_not_existing(tmpdir):
    assert list(Crawl.all(tmpdir, None)) == []


def test_all_not_directory(tmpdir):
    file = tmpdir.join('source_id')
    file.write('content')

    assert list(Crawl.all(tmpdir, None)) == []


def test_all_bad_format(tmpdir):
    tmpdir.mkdir('source_id').mkdir('directory')

    assert list(Crawl.all(tmpdir, None)) == []


@pytest.mark.parametrize('directory, expected', ([
    ('20200902_052458', datetime.datetime(2020, 9, 2, 5, 24, 58)),
    ('text', None),
]))
def test_parse_data_version(directory, expected):
    assert Crawl.parse_data_version(directory) == expected


def test_str(tmpdir):
    crawl = Crawl(tmpdir, 'scotland', data_version, None)

    assert str(crawl) == os.path.join('scotland', '20200902_052458')


def test_directory(tmpdir):
    crawl = Crawl(tmpdir, 'scotland', data_version, None)

    assert crawl.directory == str(tmpdir.join('scotland', '20200902_052458'))


def test_checksum(tmpdir):
    file = tmpdir.join('test.json')
    file.write('{"id": 1}')

    spider_directory = tmpdir.mkdir('scotland')
    file = spider_directory.join('test.json')
    file.write('{"id": 1}')

    crawl_directory = spider_directory.mkdir('20200902_052458')
    file = crawl_directory.join('test.json')
    file.write('{"id": 1}')

    sub_directory = crawl_directory.mkdir('child')
    file = sub_directory.join('test.json')
    file.write('{"id": 100}')

    crawl = Crawl(tmpdir, 'scotland', data_version, None)

    assert crawl.checksum == '16a649df237c3facf5ccd3f3e9333711fbd66ffbd1d5674afe8a6dd63f18f90d9f1fa5cae142c82f08b17f28aa3650258777ab63a6a8ac63a14b208a5562e322'  # noqa: E501


def test_checksum_empty(tmpdir):
    crawl = Crawl(tmpdir, 'scotland', data_version, None)

    assert crawl.checksum == '786a02f742015903c6c6fd852552d272912f4740e15847618a86e217f71f5419d25e1031afee585313896444934eb04b903a685b1448b755d56f701afe9be2ce'  # noqa: E501


def test_bytes(tmpdir):
    file = tmpdir.join('test.json')
    file.write('{"id": 1}')

    spider_directory = tmpdir.mkdir('scotland')
    file = spider_directory.join('test.json')
    file.write('{"id": 1}')

    crawl_directory = spider_directory.mkdir('20200902_052458')
    file = crawl_directory.join('test.json')
    file.write('{"id": 1}')  # 9

    sub_directory = crawl_directory.mkdir('child')
    file = sub_directory.join('test.json')
    file.write('{"id": 100}')  # 11

    crawl = Crawl(tmpdir, 'scotland', data_version, None)

    assert crawl.bytes == 20


def test_bytes_empty(tmpdir):
    crawl = Crawl(tmpdir, 'scotland', data_version, None)

    assert crawl.bytes == 0
