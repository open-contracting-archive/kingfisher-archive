import attr


@attr.s
class Metadata:
    version = attr.ib(default='1')
    source_id = attr.ib(default=None)
    data_version = attr.ib(default=None)
    bytes = attr.ib(default=None)
    checksum = attr.ib(default=None)
    files_count = attr.ib(default=None)
    errors_count = attr.ib(default=None)
