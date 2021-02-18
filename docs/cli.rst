.. _cli:

============================
Command Line Interface (CLI)
============================

Runner command line interface, :program:`runner`, lets the user control
a runner through a terminal.

=================   ==============================================
sub-command         description
=================   ==============================================
list-runners        list runners and their status
remove-runner       remove runner from metadata
start               start a runner from metadata
submit              submit row(s) for run.
cancel              cancel row(s) for run.
status              check running status of row(s)
graphical-status    get graphical status of the workflow for a row
=================   ==============================================

Help
====

For each sub-command, help is available as::

    $ runner --help
    $ runner <sub-command> --help

