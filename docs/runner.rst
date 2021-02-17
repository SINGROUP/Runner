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
    - `A cluster management and job scheduling system <slurm.schedmd.com>`_
  * - :class:`~runner.runners.terminal.TerminalRunner`
    - Terminal
    - A terminal based runner for simple tasks

The :class:`~runner.runner.BaseRunner` class defines basic functions for each
kind of runner.

.. autoclass:: runner.runner.BaseRunner
   :members:

Runners
=======

The :class:`~runner.runner.BaseRunner` is inherited by following implemented 
runners:

.. toctree::
   :maxdepth: 1

   runners/slurm
   runners/terminal
