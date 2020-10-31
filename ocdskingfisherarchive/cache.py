import sqlite3

from ocdskingfisherarchive.metadata import Metadata

class Cache:
    """
    A cache of which crawl directories have been archived or skipped.
    """

    def __init__(self, filename, expired=False):
        """
        :param str filename: the path to the SQLite database for caching the local state
        :param bool expired: whether to ignore and overwrite existing rows in the SQLite database
        """
        self.conn = sqlite3.connect(filename)
        self.cursor = self.conn.cursor()
        self.expired = expired

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='crawl'")
        if not self.cursor.fetchone():
            self.cursor.execute("""
                CREATE TABLE crawl (
                    id TEXT PRIMARY KEY NOT NULL,
                    source_id TEXT NOT NULL,
                    data_version TEXT NOT NULL,
                    bytes INTEGER,
                    checksum TEXT,
                    files_count INTEGER,
                    errors_count INTEGER,
                    archived BOOLEAN
                )
            """)
            self.conn.commit()

    def get(self, crawl):
        """
        :param crawl: an instance of the :class:`~ocdskingfisherarchive.crawl.Crawl` class
        :returns: the metadata for the crawl and whether the crawl was archived
        :rtype: tuple
        """
        if self.expired:
            return

        self.cursor.execute("""
            SELECT
                source_id,
                data_version,
                bytes,
                checksum,
                files_count,
                errors_count,
                archived
            FROM crawl
            WHERE id = :id
        """, {'id': crawl.identifier})
        result = self.cursor.fetchone()
        if result:
            return Metadata('1', *result[:-1]), result[-1] == 1
        return None, None

    def set(self, crawl, archived=None):
        """
        :param crawl: an instance of the :class:`~ocdskingfisherarchive.crawl.Crawl` class
        :param boolean archived: whether the crawl was archived
        """
        self.cursor.execute("""
            REPLACE INTO crawl (
                id,
                source_id,
                data_version,
                bytes,
                checksum,
                files_count,
                errors_count,
                archived
            ) VALUES (
                :id,
                :source_id,
                :data_version,
                :bytes,
                :checksum,
                :files_count,
                :errors_count,
                :archived
            )
        """, {
            'id': crawl.identifier,
            'source_id': crawl.source_id,
            'data_version': crawl.data_version,
            'bytes': crawl.bytes,
            'checksum': crawl.checksum,
            'files_count': crawl.scrapy_log_file and crawl.scrapy_log_file.item_counts['File'],
            'errors_count': crawl.scrapy_log_file and crawl.scrapy_log_file.item_counts['FileError'],
            'archived': archived,
        })
        self.conn.commit()
