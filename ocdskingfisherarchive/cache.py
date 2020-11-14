import sqlite3

from ocdskingfisherarchive.crawl import Crawl


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
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.expired = expired

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'crawl'")
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
                    reject_reason TEXT,
                    archived BOOLEAN,
                    UNIQUE (source_id, data_version)
                )
            """)
            self.conn.commit()

    def get(self, crawl):
        """
        :param crawl: an instance of the :class:`~ocdskingfisherarchive.crawl.Crawl` class
        :returns: the cached version of the given crawl, or the given crawl
        :rtype: ocdskingfisherarchive.crawl.Crawl
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
                reject_reason,
                archived
            FROM crawl
            WHERE id = :id
        """, {'id': crawl.pk})
        result = self.cursor.fetchone()
        if result:
            return Crawl(**result)
        return crawl

    def set(self, crawl):
        """
        :param crawl: an instance of the :class:`~ocdskingfisherarchive.crawl.Crawl` class
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
                reject_reason,
                archived
            ) VALUES (
                :id,
                :source_id,
                :data_version,
                :bytes,
                :checksum,
                :files_count,
                :errors_count,
                :reject_reason,
                :archived
            )
        """, crawl.asdict())
        self.conn.commit()

    def delete(self, crawl):
        """
        :param crawl: an instance of the :class:`~ocdskingfisherarchive.crawl.Crawl` class
        """
        self.cursor.execute("DELETE FROM crawl WHERE id = :id", {'id': crawl.pk})
        self.conn.commit()
