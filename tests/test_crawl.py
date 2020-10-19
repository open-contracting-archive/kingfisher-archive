import datetime

from ocdskingfisherarchive.crawl import Crawl
from tests import create_crawl_directory


def test_all(tmpdir, caplog, monkeypatch):
    create_crawl_directory(tmpdir, ['data.json'], 'log_error1.log')

    crawls = list(Crawl.all(tmpdir.join('data'), tmpdir.join('logs', 'kingfisher')))

    assert len(crawls) == 1
    assert crawls[0].source_id == 'scotland'
    assert crawls[0].data_version == datetime.datetime(2020, 9, 2, 5, 24, 58)
    assert crawls[0].data_directory == tmpdir.join('data')
