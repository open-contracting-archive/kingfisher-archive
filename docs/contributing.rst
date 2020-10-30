Contributing
============

Kingfisher Archive operates on GBs of data. We try to make it bound only by network bandwidth. To do that, we need to make efficient use of both CPU and I/O bandwidth.

In general, we try to:

-  Cache results.
-  Avoid writing intermediate results, by streaming between operations.
-  Avoid expensive calculations, by postponing them and returning early, where possible.

The expensive operations and accompanying strategies are:

Network
  Compress files.
I/O
  Archive and compress without writing an intermediate TAR file.
Compression
  Use `lz4 <https://lz4.github.io/lz4/>`__ to compress data, which has a speed of 500 MB/s per core.
Checksums
  Use `xxHash <https://cyan4973.github.io/xxHash/>`__ to calculate checksums of OCDS data, which is faster than DDR4 SDRAM's transfer rate, using AVX-512 instructions. (Find your RAM's description by running ``lshw -short -C memory`` and `look up its transfer rate <https://en.wikipedia.org/wiki/List_of_interface_bit_rates#Dynamic_random-access_memory>`__.)
Directory traversal
  Use `os.scandir() <https://docs.python.org/3/library/os.html#os.scandir>`__ instead of ``os.listdir()``.
``stat()`` system calls
  No specific optimization.

A SATA 3.2 drive has 6.0 Gb/s (750 MB/s) bandwidth, and a `Hetzner server <https://docs.hetzner.com/robot/general/traffic/>`__ has 1 Gb (125 MB/s) bandwidth: a ratio of 6:1. To not saturate the network bandwidth, compression needs to achieve a higher ratio. Using LZ4, an OCDS sample of 848 GB compresses to 121 GB, a ratio of 7:1.
