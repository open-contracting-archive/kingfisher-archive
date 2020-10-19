import sqlite3


class DataBaseArchive:
    def __init__(self, database):
        self.conn = sqlite3.connect(database)
        self.cursor = self.conn.cursor()

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='crawl'")
        if not self.cursor.fetchone():
            self.cursor.execute(
                "CREATE TABLE crawl (directory TEXT PRIMARY KEY NOT NULL, state TEXT NOT NULL)"
            )
            self.conn.commit()

    def get_state_of_crawl(self, crawl):
        state = 'UNKNOWN'
        self.cursor.execute("SELECT state FROM crawl WHERE directory = :directory", {"directory": crawl.directory})
        r = self.cursor.fetchone()
        if r:
            state = r[0]
        return state

    def set_state_of_crawl(self, crawl, state):
        self.cursor.execute("REPLACE INTO crawl (directory, state) VALUES (:directory, :state)", {
            "directory": crawl.directory, "state": state
        })
        self.conn.commit()
