========
Tutorial
========


In developing runner, we use certain terms to describe the runner:

#. Database: The `ASE database` that stores individual atomistic simulation
   data, and further data used to define the run, as rows in the database.
#. Runner: A stateless spooling utility that reads from the database, and
   manages atomistic simulation workflows using a workflow manager.
#. RunnerData: The data attached to each row of database that
   instructs the runner on how to handle the row calculation.
#. Relay: A recorder to record the computational graph, needed to perform a
   workflow. It eases setting up complex workflows, with their runnerdata,
   and their submission and data collection.

Examples
^^^^^^^^
.. toctree::
   :maxdepth: 1

   examples/CuCO

