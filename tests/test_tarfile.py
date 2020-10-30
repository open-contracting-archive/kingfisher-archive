from ocdskingfisherarchive.tarfile import LZ4TarFile
from tests import path


def test_class(tmpdir):
    compressed = tmpdir.join('compressed.lz4')
    with open(path('data.json'), 'rb') as f:
        content = f.read()

    with LZ4TarFile.open(compressed, 'w:lz4') as tar:
        tar.add(path('data.json'))

    with LZ4TarFile.open(compressed, 'r:lz4') as tar:
        for tarinfo in tar:
            assert tarinfo.name == 'tests/fixtures/data.json'
            assert tar.extractfile(tarinfo).read() == content
