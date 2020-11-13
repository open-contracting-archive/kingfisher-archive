import datetime
import os
import time

import pytest

from ocdskingfisherarchive.crawl import Crawl
from tests import assert_log, crawl, create_crawl_directory

current_time = time.time()


@pytest.mark.parametrize('mtime, expected', [
    (current_time - 604800 + 60, 0),
    (current_time - 604800 - 60, 1),  # change if tests are slow
])
def test_all(mtime, expected, tmpdir, caplog):
    create_crawl_directory(tmpdir, ['data.json'], 'log_error1.log')
    os.utime(tmpdir.join('data', 'scotland', '20200902_052458'), (mtime, mtime))

    crawls = list(Crawl.all(tmpdir.join('data'), tmpdir.join('logs', 'kingfisher')))

    assert len(crawls) == expected

    if expected:
        assert crawls[0].source_id == 'scotland'
        assert crawls[0].data_version == datetime.datetime(2020, 9, 2, 5, 24, 58)
        assert crawls[0].data_directory == tmpdir.join('data')
        assert crawls[0].scrapy_log_file.name == tmpdir.join('logs', 'kingfisher', 'scotland',
                                                             '307e8331edc801c691e21690db130256.log')
    else:
        assert_log(caplog, 'INFO', 'wait (recent) scotland/20200902_052458')


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
    crawl = Crawl('scotland', '20200902_052458', tmpdir, None)

    assert str(crawl) == os.path.join('scotland', '20200902_052458')


def test_directory(tmpdir):
    crawl = Crawl('scotland', '20200902_052458', tmpdir, None)

    assert crawl.local_directory == str(tmpdir.join('scotland', '20200902_052458'))


@pytest.mark.parametrize('data_files, log_file, expected', [
    (None, 'log1.log', 'no_data_directory'),
    ([], 'log1.log', 'no_data_files'),
    (['data.json'], None, 'no_log_file'),
    (['data.json'], 'log_in_progress1.log', 'not_finished'),
    (['data.json'], 'log_sample1.log', 'not_complete'),
    (['data.json'], 'log_not_clean_enough.log', 'not_clean_enough'),
    (['data.json'], 'log_error1.log', None),
])
def test_reject_reason(data_files, log_file, expected, tmpdir):
    create_crawl_directory(tmpdir, data_files, log_file)

    assert crawl(tmpdir).reject_reason == expected


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

    crawl = Crawl('scotland', '20200902_052458', tmpdir, None)

    assert crawl.checksum == '06bbee76269a3bd770704840395e8e10'


def test_checksum_empty(tmpdir):
    crawl = Crawl('scotland', '20200902_052458', tmpdir, None)

    assert crawl.checksum == '99aa06d3014798d86001c324468d497f'


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

    crawl = Crawl('scotland', '20200902_052458', tmpdir, None)

    assert crawl.bytes == 20


def test_bytes_empty(tmpdir):
    crawl = Crawl('scotland', '20200902_052458', tmpdir, None)

    assert crawl.bytes == 0
