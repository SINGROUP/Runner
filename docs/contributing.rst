Contributing
============

Filing an Issue
---------------

This is the way to report issues
and also to request features.

1. Go to `Runners's GitHub Issues <https://github.com/SINGROUP/Runner/issues>`_.
2. Look through open and closed issues to see if there has already been a
   similar bug report or feature request.
   `GitHub's search <https://github.com/SINGROUP/Runner/search>`_ feature may aid
   you as well.
3. If your issue doesn't appear to be a duplicate,
   `file an issue <https://github.com/SINGROUP/Runner/issues/new>`_.
4. Please be as descriptive as possible!

Pull Request
------------

1. Fork the repository `on GitHub <https://github.com/SINGROUP/Runner>`_.
2. Create a new branch for your work.
3. Install development python dependencies, by running 
   `pipenv <https://pipenv.pypa.io/en/latest/>`_ in the top-level directory with
   `Pipfile`::

   $ pipenv sync --dev
   $ pipenv shell

4. Make your changes (see below).
5. Send a GitHub Pull Request to the ``master`` branch of ``SINGROUP/Runner``.

Step 4 is different depending on if you are contributing to the code base or
documentation.

Code
^^^^

1. Run pytest in the tests folder. If any tests fail, and you
   are unable to diagnose the reason, please refer to `Filing an Issue`_.
2. Complete your patch and write tests to verify your fix or feature is working.
   Please try to reduce the size and scope of your patch to make the review
   process go smoothly.
3. Run the tests again and make any necessary changes to ensure all tests pass.
4. Run `black <https://black.readthedocs.io/en/stable/index.html>`_ to ensure correct
   linting of the python code, in the top-level directory as::

   $ black .

Documentation
^^^^^^^^^^^^^

1. Enter the `docs <https://github.com/SINGROUP/Runner/tree/master/docs>`_ directory.
2. Make your changes to any files.
3. Run ``make clean && make html``. This will generate html files in a new
   ``_build/html/`` directory.
4. Open the generated pages and make any necessary changes to the ``.rst``
   files until the documentation looks properly formatted.
