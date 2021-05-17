==================================
Relay class
==================================

``Relay`` records the workflow in a tape recorder fashion. This helps in
construction of the workflow as a directed-acyclic graph. The 
:class:`runner.relay.Relay` emphasises simplicity, speed and flexibility
in implementing the workflow.

A :class:`runner.relay.Relay` is initiated with a label, parent calculations,
runnerdata and runnername.

:Label: Just like folder-names for each calculation, a label gives a unique
        identifier to access the relay from the graph.

:Parents: Parents are 

.. autoclass:: runner.relay.Relay
   :members:
