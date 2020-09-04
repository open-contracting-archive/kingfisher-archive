import sqlite3


class DataBaseArchive:

    def __init__(self, config):
        self.config = config
        self.conn = sqlite3.connect(config.database_archive_filepath)

        c = self.conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='collections'")
        if not c.fetchone():
            c.execute("CREATE TABLE collections (collection_id INTEGER PRIMARY KEY NOT NULL, state TEXT NOT NULL)")
            c.close()
            self.conn.commit()
        else:
            c.close()

    def get_state_of_collection_id(self, collection_id):
        state = 'UNKNOWN'
        c = self.conn.cursor()
        c.execute("SELECT state FROM collections WHERE collection_id=?", str(collection_id))
        r = c.fetchone()
        if r:
            state = r[0]
        c.close()
        return state

    def set_state_of_collection_id(self, collection_id, state):
        c = self.conn.cursor()
        c.execute("REPLACE INTO collections (collection_id, state) VALUES (?, ?)", (str(collection_id), state))
        c.close()
        self.conn.commit()
