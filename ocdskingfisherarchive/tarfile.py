import tarfile

from lz4.frame import LZ4FrameFile


class LZ4TarFile(tarfile.TarFile):
    """
    .. code:: python

       from ocdskingfisherarchive.tarfile import LZ4TarFile

       with LZ4TarFile.open('compressed.lz4', 'w:lz4') as tar:
           tar.add(filename)

       with LZ4TarFile.open('compressed.lz4', 'r:lz4') as tar:
           for tarinfo in tar:
               assert tarinfo.name == filename
    """
    # See https://github.com/python/cpython/blob/3.6/Lib/tarfile.py
    OPEN_METH = {
        'lz4': 'lz4open',
    }

    @classmethod
    def lz4open(cls, name, mode='r', fileobj=None, **kwargs):
        """
        Open lz4 compressed tar archive name for reading or writing.
        """
        if mode not in ('r', 'a', 'w', 'x'):
            raise ValueError("mode must be 'r', 'a', 'w' or 'x'")

        fileobj = LZ4FrameFile(fileobj or name, mode, **kwargs)

        try:
            t = cls.taropen(name, mode, fileobj, **kwargs)
        except:  # noqa: E722
            fileobj.close()
            raise
        t._extfileobj = False
        return t
