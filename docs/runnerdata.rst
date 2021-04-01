======================
RunnerData for Runners
======================

``RunnerData`` attaches to a row in the ``ase database`` as a python 
dictionary, and is used by a ``Runner`` to run the simulation for
that row.

.. list-table:: ``RunnerData`` description
   :header-rows: 1
 
 * - Key
   - Value type
   - Description
 * - 'name'
   - str
   - Name given to the run
 * - 'scheduler_options'
   - dict
   - Data to define the workflow manager options
 * - 'parents'
   - list
   - Parent rows in the database. The runner waits for completion of these tasks
     before running the present row.
 * - 'files'
   - dict
   - Files required during the run
 * - 'tasks'
   - list
   - List of tasks, python and shell, to be performed for the run
 * - 'keep_run'
   - bool
   - Boolean indicating if the run folder is to be kept after completion

:class:`~runner.utils.runnerdata.RunnerData` is designed
to simplify the genration of this data.


:Template data: template is initialised as::

    >>> runnerdata = runner.RunnerData()
    >>> runnerdata.name = 'myEnergyRun'

:Scheduler options: are added as::

   >>> runnerdata.scheduler_options = scheduler_options

  Scheduler options are defined differently based on the workflow manager used:
  
   * :ref:`slurm_scheduler`
   * :ref:`terminal_scheduler`

.. _parents_format:

:Parents: are added as::

    >>> # setting parents as row id 2 and 3 form the same database
    >>> runnerdata.parents = [2, 3]

.. _file_format:

:Files: are added as::

    >>> runnerdata.add_file('get_energy.py')
    >>> runnerdata.add_files(['BASIS', 'POTENTIAL'])

  The files can be string or binary.

  Format for a python run file:

    * The file should have a ``main`` function, this function is called
      at execution
    * The first argument, of the ``main`` function should take a list. This
      is the list of ``atoms`` rows.
      The 0th index is the ``atoms`` row of the run, and the rest are the 
      ``atoms`` rows of the parents, in the order defined in the `parents` list.
    * The rest parameters are passed as \*\*kwargs, as defined in the `tasks`
    * The function should return an ``atoms`` object, to be added in-place at
      the row being run.
    * The `key_value_pairs`_ stored in `ase.Atoms.info` of the returned 
      ``atoms`` object, is updated in the database.
    * The ase.db.row.data is updated with the rest of `ase.Atoms.info`.

.. _key_value_pairs: https://wiki.fysik.dtu.dk/ase/ase/db/db.html#add-additional-data

.. _tasks_format:

:Tasks: ``Runner`` supports ``shell`` and ``python`` tasks.
  ``shell`` task can be added as::

    >>> runnerdata.append_tasks('shell', 'module load anaconda3')

  This will be run as::

    $ module load anaconda3

  ``Python`` task is added with the python file::

    >>> runnerdata.append_tasks('python', 'get_energy.py')

  This will be called from python code as::

    >>> from get_energy import main
    >>> main(atoms_list, **parameters)

  Here, if ``Python`` parameters are defined in the task as::
 
    >>> runnerdata.append_tasks('python', 'get_energy.py', {'param': 0})

  then, the parameters dict will pass `param`\=0 as \*\*kwargs in the main function.

  If ``Python`` task is to be executed with different python command then::

    >>> runnerdata.append_tasks('python', 'get_energy.py', {},
    ...                         'mpirun -n 4 python3')

  This will be run as::

    $ mpirun -n 4 python3 get_energy.py

.. note::

  * When adding a custom python command, without parameters, the third argument 
    has to be an empty parameter to be passed to the function.
  * Multiple python functions can be added to the task list. The returned 
    ``atoms`` object of one python task is sent as an input to the next python
    task.

:Keep run: is set as::

    >>> runnerdata.keep_run = True

.. note::

  Failed run folders are not deleted regardless of keep_run value. This aids
  in the debugging of the run.

.. autoclass:: runner.utils.runnerdata.RunnerData
   :members:
