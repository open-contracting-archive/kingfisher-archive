Use
===

The application's command-line interface uses `process locking <https://pypi.org/project/python-pidfile/>`__, to ensure that only one archival process runs at one time.

.. code-block:: shell-session

   python manage.py archive

To do a dry run:

.. code-block:: shell-session

   python manage.py archive --dry-run

To see all options:

.. code-block:: shell-session

   python manage.py archive --help
