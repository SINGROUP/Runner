About
========

Runner is a free and open source infrastructure to manage workflows of 
atomistic simulations in a high throughput fashion. It abstracts the job
submission and retrieval on workload managers, like slurm. 

:Advantages:

 * Highly robust workflows: The workflows are fault tolerant. Any failed
   run can be easily corrected, and after submission of that run, the 
   workflow resumes seamlessly.
 * Reproducible: Since all files, parameters, and functions are saved, the
   workflow becomes easily reproducible.
 * Visualisation and analysis of workflow: Due to parent-child relations
   established to determine hierarchy of runs, the visualisation and analysis
   of the workflow is easier.
 * ASE database backed: The backbone of ASE databases imply high extensibility.
