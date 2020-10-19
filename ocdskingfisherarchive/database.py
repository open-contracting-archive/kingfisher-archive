import sqlite3


class Database:
    def __init__(self, filename):
        self.conn = sqlite3.connect(filename)
        self.cursor = self.conn.cursor()

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='crawl'")
        if not self.cursor.fetchone():
            self.cursor.execute("CREATE TABLE crawl (directory TEXT PRIMARY KEY NOT NULL, state TEXT NOT NULL)")
            self.conn.commit()

    def get_state(self, crawl):
        self.cursor.execute("SELECT state FROM crawl WHERE directory = :directory", {'directory': crawl.directory})
        result = self.cursor.fetchone()
        if result:
            return result[0]
        return 'UNKNOWN'

    def set_state(self, crawl, state):
        self.cursor.execute("REPLACE INTO crawl (directory, state) VALUES (:directory, :state)", {
            'directory': crawl.directory,
            'state': state,
        })
        self.conn.commit()
