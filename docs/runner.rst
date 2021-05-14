==================================
Runner class
==================================

``Runners`` are run on systems (desktop/servers) where the atomistic 
simulations (or other related functions) are to be performed.

.. list-table:: Supported ``Runners``
   :header-rows: 1

  * - Runner name
    - Runner Type
    - Description
  * - :class:`~runner.runners.slurm.SlurmRunner`
    - Slurm
    - `A cluster management and job scheduling system 
      <https://slurm.schedmd.com>`_
  * - :class:`~runner.runners.terminal.TerminalRunner`
    - Terminal
    - A terminal based runner for simple tasks

Multiple ``Runners`` can be attached to a database, running on their
respective machines. The ``Runners`` distinguish their rows by the 
`runner` `key_value_pairs 
<https://wiki.fysik.dtu.dk/ase/ase/db/db.html#add-additional-data>`_
of the database row.

The :class:`~runner.runner.BaseRunner` class defines basic functions for each
kind of runner.

.. autoclass:: runner.runner.BaseRunner
   :members:

Status of run
=============

The ``Runner`` runs based on a `status` `key_value_pairs 
<https://wiki.fysik.dtu.dk/ase/ase/db/db.html#add-additional-data>`_
in the database.
The status is given as:

.. list-table:: Status
    :header-rows: 1

  * - Status
    - Description
  * - 'submit'
    - Indicates the ``Runner`` to submit the run, using ``RunnerData``.
  * - 'cancel'
    - Indicates the ``Runner`` to cancel the run.
  * - 'running'
    - ``Runner`` indicates that the row function is running.
  * - 'failed'
    - ``Runner`` indicates that the row function has failed.
  * - 'done'
    - ``Runner`` indicates that the row function has completed.


.. note::

  When the status is set to cancel, the ``Runner`` changes the status to 
  'failed' after cancelling the run.

Runners
=======

The :class:`~runner.runner.BaseRunner` is inherited by following implemented 
runners:

.. toctree::
   :maxdepth: 1

   runners/slurm
   runners/terminal
