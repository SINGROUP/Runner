Terminal
========

.. autoclass:: runner.runners.terminal.TerminalRunner
   :members:
   :show-inheritance:

Similarily a ``terminal runner`` can be setup::

   >>> from runner import TerminalRunner
   >>> runner_ = TerminalRunner('myRunner',
   ...                          'myDatabase.db',
   ...                          tasks=[['shell', 'module load anaconda3']],
   ...                          max_jobs=5,
   ...                          cycle_time=30,
   ...                          keep_run=False,
   ...                          run_folder='./')

This runner can start spooling via::

   >>> runner_.spool()

or the runner can be attached to the database::

   >>> runner_.to_database()

to be run from the respective machine::

   >>> runner_ = TerminalRunner.from_database('terminal:myRunner',
   ...                                        'myDatabase.db')
   >>> runner_.spool()

or can be run via :ref:`cli` tools.

.. note::
  Terminal tasks are run as a child process of the ``TerminalRunner`` itself, 
  and hence, are killed when ``TerminalRunner`` is stopped. This is an 
  unwanted behaviour, and will be rectified in the future release.

.. _terminal_scheduler:

Terminal scheduler options in ``RunnerData``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Scheduler options are not yet implemented in ``TermianlRunner``


