import sqlite3


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
            self.cursor.execute("CREATE TABLE crawl (directory TEXT PRIMARY KEY NOT NULL, state TEXT NOT NULL)")
            self.conn.commit()

    def get(self, crawl):
        """
        :param crawl: an instance of the :class:`~ocdskingfisherarchive.crawl.Crawl` class
        :returns: the state of the crawl directory, if any
        :rtype: str
        """
        if self.expired:
            return

        self.cursor.execute("SELECT state FROM crawl WHERE directory = :directory", {'directory': crawl.directory})
        result = self.cursor.fetchone()
        if result:
            return result[0]

    def set(self, crawl, state):
        """
        :param crawl: an instance of the :class:`~ocdskingfisherarchive.crawl.Crawl` class
        :param str state: the state to which to set the crawl directory
        """
        self.cursor.execute("REPLACE INTO crawl (directory, state) VALUES (:directory, :state)", {
            'directory': crawl.directory,
            'state': state,
        })
        self.conn.commit()
