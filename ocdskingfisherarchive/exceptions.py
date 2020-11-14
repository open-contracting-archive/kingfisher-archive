class KingfisherArchiveError(Exception):
    """Base class for exceptions from within this package"""


class SourceMismatchError(KingfisherArchiveError):
    """Raised if two crawls with different sources are compared"""


class FutureDataVersionError(KingfisherArchiveError):
    """Raised if a future crawl is compared with a given crawl"""
