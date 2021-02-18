====================
Installation
====================

Requirements
====================

* Python_ 3.6 or newer
* ASE_ 3.19 or newer

.. _Python: https://www.python.org/
.. _ASE: https://wiki.fysik.dtu.dk/ase/index.html

Installation from source
=========================

We currently only test on Linux systems. On windows we recommend the `Windows
Subsystem for Linux (WSL)
<https://en.wikipedia.org/wiki/Windows_Subsystem_for_Linux>`_. The exact list
of dependencies are given in setup.py and all of them will be automatically
installed during setup.

:git:

The source is available on `github <https://github.com/SINGROUP/Runner>`_,
which can be installed as::

    $ git clone https://github.com/SINGROUP/Runner.git
    $ cd Runner
    $ pip install . --upgrade


To update the installation::

    $ cd Runner
    $ git pull
    $ pip install . --upgrade
