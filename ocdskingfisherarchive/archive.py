
class Archive:

    def __init__(self, config):
        self.config = config

    def archive(self):
        print("ARCHIVING TODO FROM DATABASE " + self.config.database_uri)
