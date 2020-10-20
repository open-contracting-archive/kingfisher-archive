Use
===

Archive
-------

The application's command-line interface uses `process locking <https://pypi.org/project/python-pidfile/>`__, to ensure that only one archival process runs at one time.

.. code-block:: shell-session

   python manage.py archive

To do a dry run:

.. code-block:: shell-session

   python manage.py archive --dry-run

To see all options:

.. code-block:: shell-session

   python manage.py archive --help

Restore
-------

#. Access Amazon S3
#. Find and download the archive
#. Uncompress the archive, for example:

   .. code-block:: shell-session

      unlz4 source.tar.lz4

#. Extract the files, for example:

   .. code-block:: shell-session

      tar xvf source.tar

#. Load the files into Kingfisher Process
#. Delete the files, for example:

   .. code-block:: shell-session

      rm -f source.tar.lz4 source.tar
      rm -rf source/20200102_030405
      rmdir source

If you downloaded multiple archives for the same source, the above commands will only delete the individual archive.

.. note::

   Do not extract the files into Kingfisher Collect's ``FILES_STORE`` directory. Otherwise, they risk being archived again!
