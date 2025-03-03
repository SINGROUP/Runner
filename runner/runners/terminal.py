import subprocess as sb
from _datetime import datetime

from runner.runner import BaseRunner


class TerminalRunner(BaseRunner):
    """
    Terminal Runner

    Args:
        name (str): The name of the runner. It is saved as
            `terminal:<given_name>`. Used to identify from other runners
            attached to the database.
        database (str): ASE database to connect
        interpreter (str): the interpreter for the shell
        pre_runner_data (:class:`RunnerData`): pre-run runnerdata
            Files, tasks, and scheduler_options can be added to be added
            to all the runs handled by this runner.
        max_jobs (int): maximum number of jobs running at an instance
        cycle_time (int): time in seconds
        keep_run (bool): keep the folder in which the run was performed
        run_folder (str): the folder that needs to be populated
        multi_fail (int): The number of re-runs on failure
        logfile (str): The log filename for logging
    """

    def __init__(
        self,
        name,
        database="database.db",
        interpreter="#!/bin/bash",
        pre_runner_data=None,
        max_jobs=50,
        cycle_time=30,
        keep_run=False,
        run_folder="./",
        multi_fail=0,
        logfile=None,
    ):
        if not name.startswith("terminal:"):
            name = "terminal:" + name
        super().__init__(
            name=name,
            database=database,
            interpreter=interpreter,
            pre_runner_data=pre_runner_data,
            max_jobs=max_jobs,
            cycle_time=cycle_time,
            keep_run=keep_run,
            run_folder=run_folder,
            multi_fail=multi_fail,
            logfile=logfile,
        )

    def _submit(self, tasks, scheduler_options):
        """
        Submit job

        Args:
            tasks (list): list of tasks to be added
            scheduler_options (dictionary): dictionary of headers to be added

        Returns:
            str: Job id of the successful run, None if failed
            str: log message of the run
        """
        # default values
        job_id = None

        log_msg = "{}\nSubmission using {} scheduler\n".format(
            datetime.now(), self.name
        )
        # add start status file
        with open("status.txt", "w") as f:
            f.write("start\n")

        # add interpreter
        run_script = "{}\n".format(self.interpreter)

        # make script escape if error
        run_script += "set -e\n"

        # add tasks
        run_script += "\n".join(tasks)

        # add done status file on completion
        run_script += "\necho done > status.txt\n"

        with open("run.sh", "w") as f:
            f.write(run_script)

        out = sb.run(["chmod", "+x", "run.sh"], stderr=sb.PIPE, stdout=sb.PIPE)
        if out.returncode == 0:
            out1 = sb.Popen(["./run.sh", ">", "run.out"])
            # successful submission
            job_id = out1.pid
            log_msg += "Submitted batch job {}\n".format(job_id)
        else:
            # failed
            log_msg += "Submission failed: {}\n".format(out.stderr.decode("utf-8"))
        return job_id, log_msg

    def _cancel(self, job_id):
        """
        Cancels job_id
        """
        import psutil as ps

        if job_id is not None:
            try:
                process = ps.Process(int(job_id))
                process.kill()
            except ps.NoSuchProcess:
                pass

    def _status(self, job_id):
        """
        return status of job_id

        Args:
            job_id (str): job id of the run

        Returns:
            str: status of the job id
            str: log message of the last change
        """
        status = "running"
        log_msg = ""
        import psutil as ps

        try:
            process = ps.Process(int(job_id))
            if process.is_running():
                # if its a zombie, process is still running
                cmdline = process.cmdline()
                if len(cmdline) == 0:
                    status = "done"
            else:
                status = "done"
        except ps.NoSuchProcess:
            status = "done"

        with open("status.txt", "r") as f:
            lines = f.readlines()[0].strip()
            if lines != "done" and status == "done":
                status = "failed"

        if status == "done":
            log_msg += "{}\n Job finished.".format(datetime.now())
        elif status == "failed":
            log_msg += "{}\n Job failed".format(datetime.now())

        return status, log_msg
