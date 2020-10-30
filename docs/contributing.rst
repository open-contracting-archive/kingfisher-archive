Contributing
============

Kingfisher Archive operates on GBs of data. As such, we try to make it bound by I/O only: that is, reading/writing files.

In general, we try to:

-  Cache the results of calculations.
-  Avoid writing intermediate results, by streaming between operations.
-  Avoid expensive calculations, by postponing them and returning early, where possible.

The most expensive local operations are:

I/O
  Archive and compress without writing an intermediate TAR file.
Compression
  Use `lz4 <https://lz4.github.io/lz4/>`__ to compress data, which has a speed of 500 MB/s per core.
Checksums
  Use `xxHash <https://cyan4973.github.io/xxHash/>`__ to calculate checksums of OCDS data, which is faster than DDR4 SDRAM's transfer rate, using AVX-512 instructions. (Find your RAM's description by running ``lshw -short -C memory`` and `look up its transfer rate <https://en.wikipedia.org/wiki/List_of_interface_bit_rates#Dynamic_random-access_memory>`__.)
``stat()`` system calls
  No specific optimization.
Directory traversal
  Use `os.scandir() <https://docs.python.org/3/library/os.html#os.scandir>`__ instead of ``os.listdir()``.

For reference, SATA 3.2 is 6.0 Gb/s or 750 MB/s.
