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
:ref:`status <Status of run>` of the run.

The :class:`~runner.runner.BaseRunner` class defines basic functions for each
kind of runner.

.. autoclass:: runner.runner.BaseRunner
   :members:

Status of run
=============

The ``Runner`` runs based on a `status` `key_value_pairs 
<https://wiki.fysik.dtu.dk/ase/ase/db/db.html#add-additional-data>`_
in the database.
When denoting the status the runner type and name is also appended. For example,
a run presently running on :class:`~runner.runners.terminal.TerminalRunner`
named 'myRunner' will be denoted as `running:terminal:myRunner`. Similarily,
for other statuses and :class:`~runner.runners.slurm.SlurmRunner`.
The status is given as:

.. list-table:: Status
    :header-rows: 1

  * - Status
    - Description
  * - 'submit:<runner_type>:<runner_name>'
    - Indicates the ``Runner`` to submit the run, using ``RunnerData``.
  * - 'cancel:<runner_type>:<runner_name>'
    - Indicates the ``Runner`` to cancel the run.
  * - 'running:<runner_type>:<runner_name>'
    - ``Runner`` indicates that the row function is running.
  * - 'failed:<runner_type>:<runner_name>'
    - ``Runner`` indicates that the row function has failed.
  * - 'done:<runner_type>:<runner_name>'
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
