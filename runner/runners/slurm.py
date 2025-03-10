import subprocess as sb
from datetime import datetime

from runner.runner import BaseRunner

# List of states from the man page of squeue
# states are mapped to status and log message
_slurm_map = {
    "CANCELLED": [
        "failed",
        "Job  was explicitly cancelled by the user or "
        "system administrator.  The job may or may "
        "not  have  been initiated.",
    ],
    "COMPLETED": ["done", "Job has terminated all processes on all nodes."],
    "CONFIGURING": [
        "running",
        "Job  has  been allocated resources, but are waiting for them to become ready "
        "for use (e.g. booting).",
    ],
    "COMPLETING": [
        "running",
        "Job is in the process of completing. Some processes on some nodes may still "
        "be active.",
    ],
    "FAILED": [
        "failed",
        "Job  terminated  with  non-zero  exit code or other failure condition.",
    ],
    "NODE_FAIL": [
        "failed",
        "Job terminated due to failure of one or more allocated resources",
    ],
    "PENDING": ["running", "Job is awaiting resource allocation."],
    "PREEMPTED": ["failed", "Job terminated due to preemption."],
    "RUNNING": ["running", "Job currently has an allocation."],
    "SUSPENDED": [
        "running",
        "Job  has an allocation, but execution has been suspended",
    ],
    "TIMEOUT": ["failed", "Job terminated upon reaching its time limit."],
}


class SlurmRunner(BaseRunner):
    """
    Slurm runner

    Args:
        name (str): The name of the runner. It is saved as
            `slurm:<given_name>`. Used to identify from other runners attached
            to the database.
        database (str): ASE database to connect
        interpreter (str): the interpreter for the shell
        pre_runner_data (:class:`RunnerData`): pre-run runnerdata
            Files, tasks, and scheduler_options can be added to all the runs
            handled by this runner.
        max_jobs (int): maximum number of jobs running at an instance
        cycle_time (int): time in seconds
        keep_run (bool): keep the folder in which the run was performed
        run_folder (str): the folder that needs to be populated
        multi_fail (int): The number of re-runs on failure
        logfile (str): the log filename for logging
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
        if not name.startswith("slurm:"):
            name = "slurm:" + name
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
        # add interpreter
        run_script = "{}\n".format(self.interpreter)

        # add SBATCH options
        for key, value in scheduler_options.items():
            run_script += "#SBATCH {}{}{}\n".format(
                key, "=" if key.startswith("--") else " ", value
            )

        run_script += "\n"

        # make script escape if error
        run_script += "set -e\n"

        # add tasks
        run_script += "\n".join(tasks)

        with open("batch.slrm", "w") as fio:
            fio.write(run_script)

        out = sb.run(["sbatch", "batch.slrm"], stderr=sb.PIPE, stdout=sb.PIPE)
        if out.returncode == 0:
            # successful submission
            job_id = out.stdout.decode("utf-8").split()[-1]
            log_msg += "Submitted batch job {}\n".format(job_id)
        else:
            # failed
            log_msg += "Submission failed: {}\n".format(out.stderr.decode("utf-8"))
        return job_id, log_msg

    def _cancel(self, job_id):
        """
        Cancels job_id
        """
        if job_id is not None:
            sb.run(["scancel", job_id])

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
        out = sb.run(
            [
                "sacct",
                "-j",
                job_id,
                "--format",
                "JobName",
                "--format",
                "State",
                "--format",
                "End",
                "--format",
                "Elapsed",
                "--format",
                "CPUTime",
                "--parsable",
            ],
            stdout=sb.PIPE,
            stderr=sb.PIPE,
        )
        # get formatted output
        formatted_out = [x.split("|") for x in out.stdout.decode("utf-8").split("\n")]
        end_time = formatted_out[1][2].replace("T", " ")
        cpu_time = formatted_out[1][4]

        # slurm state of the job
        # state_list = [x[1].split()[0] for x in formatted_out[1:-1]]
        # better handling of the gibberish last line
        state_list = []
        for x in formatted_out[1:]:
            try:
                state_list.append(x[1].split()[0])
            except IndexError:
                # the last line in formatted_out is gibberish sometimes
                # ignore if it is not as per the | format
                pass
        # scheduler status of the job
        status_list = []
        for state in state_list:
            try:
                status_list.append(_slurm_map[state][0])
            except KeyError:
                status = "failed"
                log_msg += "{}\n Undefined slurm state:{}\n".format(end_time, state)
                return status, log_msg

        if "failed" in status_list:
            state = state_list[status_list.index("failed")]
            status = "failed"
            log_msg += "{}\n{} {}\n".format(end_time, state, _slurm_map[state][1])
        elif "running" in status_list:
            status = "running"
            log_msg += ""
        else:
            # done
            status = "done"
            log_msg += "{}\n Job finished.\nWall time={}".format(end_time, cpu_time)

        return status, log_msg
