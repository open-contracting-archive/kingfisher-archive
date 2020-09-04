import sqlalchemy as sa
from ocdskingfisherarchive.collection import Collection


class DataBaseProcess:

    def __init__(self, config):
        self.config = config
        self._engine = None

    def get_engine(self):
        if not self._engine:
            self._engine = sa.create_engine(
                self.config.database_uri,
            )
        return self._engine

    def get_collections_to_consider_archiving(self):
        sql = "SELECT * FROM collection WHERE sample IS FALSE AND store_end_at IS NOT NULL AND transform_type = ''"
        collections = []
        with self.get_engine().begin() as connection:
            for row in connection.execute(sa.sql.expression.text(sql)):
                collections.append(
                    Collection(
                        config=self.config,
                        database_id=row['id'],
                        source_id=row['source_id'],
                        data_version=row['data_version'],
                    )
                )
        return collections
