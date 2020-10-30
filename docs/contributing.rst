Contributing
============

Kingfisher Archive operates on GBs of data. As such, it tries to be bound only by I/O (for reference, SATA 3.2 is 6.0 Gb/s or 750 MB/s), by doing the following:

-  Cache the results of calculations.
-  Avoid expensive calculations, by postponing them and returning early, where possible.
-  Use `os.scandir() <https://docs.python.org/3/library/os.html#os.scandir>`__ instead of ``os.listdir()``.
-  Use `lz4 <https://lz4.github.io/lz4/>`__ to compress data, which has a speed of 500 MB/s per core.
-  Use `xxHash <https://cyan4973.github.io/xxHash/>`__ to calculate checksums of OCDS data, which is faster than DDR4 SDRAM's transfer rate, using AVX-512 instructions. (Find your RAM's description by running ``lshw -short -C memory`` and `look up its transfer rate <https://en.wikipedia.org/wiki/List_of_interface_bit_rates#Dynamic_random-access_memory>`__.)

