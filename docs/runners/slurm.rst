Slurm
=====

.. autoclass:: runner.runners.slurm.SlurmRunner
   :members:
   :show-inheritance:

Setting up a ``slurm runner``::

   from runner import SlurmRunner
   runner_ = SlurmRunner('myRunner',
                         'myDatabase.db',
                         scheduler_options={'--account': 'myAccount'},
                         tasks=[['shell', 'module load anaconda3']],
                         max_jobs=5,
                         cycle_time=30,
                         keep_run=False,
                         run_folder='./')

This runner can start spooling via::

    runner_.spool()

or the runner can be attached to the database::

    runner_.to_database()

to be run from the respective machine::

    runner_ = SlurmRunner.from_database('myRunner', 'myDatabase.db')
    runner_.spool()

or can be run via :ref:`cli` tools.

.. _slurm_scheduler:

Slurm scheduler options in ``RunnerData``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Schduler options for slurm are placed in the slurm file as ``#SBATCH`` options::

    >>> scheduler_data = {'-N': 1,
    ...                   '-n': 16,
    ...                   '-t': '0:5:0:0',
    ...                   '--mem-per-cpu': 2000}

This will result in a slurm file as::

    #SBATCH -N 1
    #SBATCH -n 16
    #SBATCH -t 0:5:0:0
    #SBATCH --mem-per-cpu=2000

See https://slurm.schedmd.com/pdfs/summary.pdf for further options.
