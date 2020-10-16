Use
===

This will run with `process locking <https://pypi.org/project/python-pidfile/>`__ so only one instance of this can run at once. (There are algorithms to decide whether to back something up based on what has already been backed up, and these may get confused if 2 processes run at once.)

.. code-block:: shell-session

   python manage.py archive

You con run a test mode (“dry mode”):

.. code-block:: shell-session

   python manage.py archive --dry-run

To see options (including a dry run)

.. code-block:: shell-session

   python manage.py archive --help
