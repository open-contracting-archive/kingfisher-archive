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


def test_all_spider_file(tmpdir):
    file = tmpdir.join('source_id')
    file.write('content')

    assert list(Crawl.all(tmpdir, None)) == []


def test_all_spider_sample(tmpdir):
    create_crawl_directory(tmpdir, ['data.json'], 'log_error1.log', source_id='scotland_sample')

    assert list(Crawl.all(tmpdir.join('data'), tmpdir.join('logs', 'kingfisher'))) == []


def test_all_crawl_file(tmpdir):
    file = tmpdir.mkdir('source_id').join('20200902_052458')
    file.write('content')

    assert list(Crawl.all(tmpdir, None)) == []


def test_all_crawl_format(tmpdir):
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

    assert crawl.checksum == 'e274ef4fcecec183c7f24da36f81de8d3d007fa97cc99fd3d12126261f90adc6'


def test_checksum_empty(tmpdir):
    crawl = Crawl(tmpdir, 'scotland', data_version, None)

    assert crawl.checksum == 'af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262'


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
