=================
Cu-CO bond length
=================

In Non-contact AFM, the tip-CO interaction is simulated by two Cu
atoms followed by CO (Cu-Cu-C-O). We will take a simple example
where we first relax the Cu-Cu atoms and the CO molecule. Then we will 
bring them together at certain distances, to finally compute the lowest
energy distance.

#. Setting up the Runner
========================

First step is to setup and start a runner. This will look for jobs to be 
submitted, and will submit them. This is also responsible for checking the
status of the run, and update the status as failed or done, accordingly.

For the demonstration, a simple terminal runner is defined from 
:class:`~runner.runners.terminal.TerminalRunner`::

    >>> from runner import TerminalRunner

    >>> runner = TerminalRunner('terminal:myRunner',
    >>>                         'database.db',
    >>>                         max_jobs=5,
    >>>                         cycle_time=5,
    >>>                         keep_run=False,
    >>>                         run_folder='./')
    >>> runner.to_database()

To start the runner we can, on the machine the run takes place, execute::

    >>> from runner import TerminalRunner

    >>> runner = TerminalRunner.from_database('terminal:myRunner',
    ...                                       'database.db')
    >>> runner.spool()

or can be run via :ref:`cli` tools::

    $ runner list-runners -db database.db
                          Runner name                         Status 
     ===============================================================
     1              terminal:myRunner                    not running
    $ runner start terminal:myRunner -db database.db
    ...

.. note::
    `Screen 
    <https://www.geeksforgeeks.org/screen-command-in-linux-with-examples/>`_
    can be used to run a shell session in the background. This can safely run
    the runner without the concern of terminating the session on logout.


#. Defining workflow
====================

Setting up the RunnerData
^^^^^^^^^^^^^^^^^^^^^^^^^

A ``RunnerData`` to define the relaxation run be now defined. The python run file
follows the :ref:`file format <file_format>`. Following the format, we can make the
BFGS function file as:

.. literalinclude:: /examples/BFGS.py
  :language: python

This can be included with the ``RunnerData`` as::

    >>> from runner import RunnerData
    >>> # initialise template
    >>> bfgs_rundata = RunnerData('BFGS')
    >>> # add relevant files
    >>> bfgs_rundata.add_file('BFGS.py')
    >>> # add tasks to be performed
    >>> bfgs_rundata.append_tasks('python', 'BFGS.py', {'fmax': 0.05})

Similarily, we can add ``RunnerData`` for interaction energy and finding
minimum energy configuration.

.. literalinclude:: /examples/interaction_energy.py
  :language: python

.. literalinclude:: /examples/min_energy.py
  :language: python

These can be included with the ``RunnerData`` as::

    >>> # initialise template
    >>> int_rundata = RunnerData('interaction_energy')
    >>> # add the run function file
    >>> int_rundata.add_file('interaction_energy.py')
    >>> # add python task with parameter
    >>> int_rundata.append_tasks('python', 'interaction_energy.py', {'d': 1})

    >>> # template runnerdata
    >>> min_rundata = RunnerData('min_energy')
    >>> # adding python function file
    >>> min_rundata.add_file('min_energy.py')
    >>> # adding python task
    >>> min_rundata.append_tasks('python', 'min_energy.py')


Setting up the Relay
^^^^^^^^^^^^^^^^^^^^

We start with the Cu-Cu and CO molecules with
appropriate calculators, and prepare the workflow as a relay.
We'll use `ase.calculators.EMT 
<https://wiki.fysik.dtu.dk/ase/ase/calculators/emt.html#module-ase.calculators.emt>`_ 
for simple demonstration::

    >>> from ase import Atoms
    >>> from ase.calculators.emt import EMT
    >>> from ase.constraints import FixAtoms

    >>> # CO
    >>> co = Atoms('CO', positions=[[0, 0, 0], [0, 0, 1]])
    >>> # fixing C atom
    >>> cons = FixAtoms(indices=[0])
    >>> co.set_constraint(cons)
    >>> # adding calculator
    >>> calc = EMT()
    >>> co.set_calculator(calc)
    >>> # Cu-Cu
    >>> cu = Atoms('Cu2', positions=[[0, 0, 0], [0, 0, 1]])
    >>> # fixing one Cu atom
    >>> cons = FixAtoms(indices=[1])
    >>> cu.set_constraint(cons)
    >>> # adding calculator
    >>> calc = EMT()
    >>> cu.set_calculator(calc)

    >>> # now these can be made into relay
    >>> from runner import Relay
    >>> co_relay = Relay('co', co, bfgs_rundata, 'terminal:myRunner')
    >>> cu_relay = Relay('cu', cu, bfgs_rundata, 'terminal:myRunner')

    >>> data_list = [x for x in range(1, 10)]
    >>> relays = []
    >>> for i, dist in enumerate(data_list):
    >>>     int_rundata.tasks[0][2]['d'] = dist
    >>>     relays.append(Relay(f'd:{dist}', [co_relay, cu_relay],
    ...                         int_rundata, 'terminal:myRunner'))

    >>> min_data = Relay('final', relays, min_rundata, 'terminal:myRunner')

#. Submitting the relay
=======================

To mark the rows for submission for ``Runner``, the relay is first commited.
This adds the workflow to the database. Then the relay can be started::

    >>> min_data.commit('database.db')
    >>> min_data.start()

or can be run via :ref:`cli` tools.

When the rows are submited, the ``Runner`` starts managing all the runs
accordingly.

.. figure:: /examples/ase_database.png

    Snapshot of `ASE database browser GUI <https://wiki.fysik.dtu.dk/ase/ase/db/db.html#browse-database-with-your-web-browser>`_ after completing all runs

#. Graphical visualisation of the workflow
==========================================

We can visualise the graph of the workflow made through the ``Relay``
, while the ``Runner`` runs the rows::

    >>> min_data.get_relay_graph('graph.png', add_tasks=True)

or can be run via :ref:`cli` tools.

.. figure:: /examples/graph.png

   Snapshot of the graph of the entire workflow needed to generate the last 
   row, after completing all runs.
