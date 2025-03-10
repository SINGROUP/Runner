"""
Base runner for different runnners
"""

import json
import logging
import os
import pickle
import shutil
import time
from abc import ABC, abstractmethod
from base64 import b64decode
from datetime import datetime

from ase import Atoms, db

from runner.utils import Cd, run
from runner.utils.runnerdata import RunnerData

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s:%(name)s:%(message)s")

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(stream_handler)


class BaseRunner(ABC):
    """
    Runner runs tasks

    Args:
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
        logfile (str): log file for logging
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
        # logging
        if logfile:
            file_handler = logging.FileHandler(logfile)
            file_handler.setLevel(logging.ERROR)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        logger.debug("Initialising")
        self.name = name
        self.database = database
        self.fdb = db.connect(database)
        self.max_jobs = max_jobs
        self.cycle_time = cycle_time
        self.keep_run = keep_run
        self.run_folder = os.path.abspath(run_folder)
        self.multi_fail = multi_fail
        self.interpreter = interpreter

        if pre_runner_data is None:
            self.pre_runner_data = RunnerData()
        else:
            self.pre_runner_data = pre_runner_data

    def to_database(self, update=False):
        """attaches runner to database

        Args:
            update (bool): optional, update runner info if already exists"""
        dict_ = {}
        dict_["max_jobs"] = self.max_jobs
        dict_["cycle_time"] = self.cycle_time
        dict_["keep_run"] = self.keep_run
        dict_["run_folder"] = self.run_folder
        dict_["multi_fail"] = self.multi_fail
        dict_["interpreter"] = self.interpreter
        dict_["pre_runner_data"] = self.pre_runner_data.data
        dict_["running"] = False

        # get present metadata
        with db.connect(self.database) as fdb:
            meta = fdb.metadata

        runners = meta.get("runners", {})

        if self.name in runners:
            if not update:
                raise RuntimeError(
                    "Runner exists, pass argument update as true to update"
                )
            if runners[self.name].get("running", False):
                raise RuntimeError("Runner already running")

        if "runners" not in meta:
            meta["runners"] = {}

        meta["runners"].update({self.name: dict_})
        self.fdb.metadata = meta

    @classmethod
    def from_database(cls, name, database):
        """Get runner from database

        Args:
            name (str): name of runner
            database (str): database

        Returns:
            :class:`~runner.runner.BaseRunner`: returns relevant runner class
        """

        fdb = db.connect(database)
        meta = fdb.metadata

        try:
            dict_ = meta["runners"][name]
        except KeyError:
            raise KeyError(f"{name} not in runners, try runner list")
        dict_.pop("running", False)
        dict_.pop("_explicit_stop", False)
        _ = RunnerData.from_data_dict(dict_["pre_runner_data"])
        dict_["pre_runner_data"] = _

        return cls(name=name, database=database, **dict_)

    def _set_running(self):
        """notify database that runner is running"""
        # get present metadata
        with db.connect(self.database) as fdb:
            meta = fdb.metadata
        meta["runners"][self.name]["running"] = True
        self.fdb.metadata = meta

    def _unset_running(self):
        """notify database that runner is not running"""
        # get present metadata
        with db.connect(self.database) as fdb:
            meta = fdb.metadata
        if self.name in meta["runners"]:
            # in case runner removed forcefully
            meta["runners"][self.name]["running"] = False
            # remove _explicit_stop bool, if present
            meta["runners"][self.name].pop("_explicit_stop", None)
            self.fdb.metadata = meta

    def get_job_id(self, input_id):
        """
        Returns job id (slurm id, process id etc) depending on workflow
        manager of a running row.

        Args:
            input_id (int): Row id

        Returns:
            int or None: job_id of input_id if running, else None
        """
        try:
            with Cd(self.run_folder, mkdir=False):
                with Cd(str(input_id), mkdir=False):
                    with open("job.id") as file_o:
                        job_id = file_o.readline().strip()
            return job_id
        except FileNotFoundError:
            return None

    @abstractmethod
    def _submit(self, tasks, scheduler_options):
        """
        Abstract method to define submit

        Args:
            tasks (list): list of tasks
            scheduler_options (dict): dictionary of headers to be added

        Returns:
            str: Job id of the successful run, None if failed
            str: log message of the run
        """
        pass

    @abstractmethod
    def _cancel(self, job_id):
        """
        cancel job id, if job id doesn't exist
        then do nothing and raise no error

        Args:
            job_id (str or None): job id to cancel

        Returns:
            None
        """
        pass

    @abstractmethod
    def _status(self, job_id):
        """
        return status of job_id

        Args:
            job_id (str): job id of the run

        Returns:
            str: status of the job id
            str: log message of the last change
        """
        pass

    def _update_status_running(self):
        """
        changes running to failed or done if finished
        """
        # get status of running jobs
        update_ids_status = {}
        for row in self.fdb.select(
            status="running", runner=f"{self.name}", columns=["id"], include_data=False
        ):
            id_ = row.id
            logger.debug("getting job id {}".format(id_))
            job_id = self.get_job_id(id_)
            if job_id:
                logger.debug("job_id success; getting status")
                # !TODO: update scheduler options with cpu usage
                with Cd(self.run_folder, mkdir=False):
                    with Cd(str(id_), mkdir=False):
                        status, log_msg = self._status(job_id)
            else:
                # oops
                logger.debug("job_id fail; updating status")
                status, atoms, log_msg = [
                    "failed",
                    None,
                    "{}\nJob id lost\n".format(datetime.now()),
                ]
            if status != "running":
                # if not still running, update status and add log message
                update_ids_status[id_] = [status, log_msg]

        # update status for jobs that stopped running
        for id_, values in update_ids_status.items():
            status, log_msg = values
            logger.debug("ID {} finished".format(id_))

            if status == "done":
                with Cd(self.run_folder, mkdir=False):
                    with Cd(str(id_), mkdir=False):
                        try:
                            # !TODO: remove reliance on pickle
                            with open("atoms.pkl", "rb") as file_o:
                                atoms = pickle.load(file_o)
                            # make sure atoms is not list
                            if isinstance(atoms, list):
                                atoms = atoms[0]
                            assert isinstance(atoms, Atoms)
                        except Exception as e:
                            status = "failed"
                            log_msg += "{}\n Unpickling failed: {}\n".format(
                                datetime.now(), e
                            )
            # run post-tasks
            if status == "done":
                logger.debug("status: done")
                # getting data
                logger.debug("getting data")
                data = self.fdb.get(id_).data

                # updating status and log
                _ = data["runner"].get("log", "") + log_msg
                data["runner"]["log"] = _
                # remove old data
                atoms.info.pop("data", None)
                atoms.info.pop("unique_id", None)
                key_value_pairs = atoms.info.pop("key_value_pairs", {})
                # update data
                key_value_pairs["status"] = status
                data.update(atoms.info)
                logger.debug("updating")
                self.fdb.update(id_, atoms=atoms, data=data, **key_value_pairs)
                # delete run if keep_run is False
                if not self.keep_run and not data["runner"].get("keep_run", False):
                    with Cd(self.run_folder):
                        if str(id_) in os.listdir():
                            shutil.rmtree(str(id_))
            else:
                logger.debug("status:failed")
                # getting data
                logger.debug("getting data")
                data = self.fdb.get(id_).data

                if status == "failed":
                    if "fail_count" not in data["runner"]:
                        data["runner"]["fail_count"] = 1
                    else:
                        data["runner"]["fail_count"] += 1

                # updating status and log
                _ = data["runner"].get("log", "") + log_msg
                data["runner"]["log"] = _
                logger.debug("updating")
                self.fdb.update(id_, status=status, data=data)

            # print status
            logger.info("Id {} finished with status: {}".format(id_, status))

    def get_status(self):
        """
        Returns ids of each status

        Returns:
            dict: dictionary of status, ids list
        """
        # status dict with ids in different status
        status_dict = {
            "done": [],  # job done
            "running": [],  # job running
            "pending": [],  # job pending, future implementation
            "failed": [],  # job failed
            "cancel": [],  # job cancelled
            "submit": [],
        }  # job submitted

        for status in status_dict:
            for row in self.fdb.select(
                status=status, columns=["id"], runner=self.name, include_data=False
            ):
                status_dict[status].append(row.id)

        return status_dict

    def _submit_run(self):
        """
        submits runs
        """
        len_running = self.fdb.count(status="running", runner=f"{self.name}")
        submit_ids = []
        for row in self.fdb.select(
            status="submit", runner=f"{self.name}", columns=["id"], include_data=False
        ):
            submit_ids.append(row.id)
        # submiting pending jobs
        sent_jobs = 0
        for id_ in submit_ids:
            row = self.fdb.get(id_)
            logger.debug("submit {}".format(id_))
            # default status, no submission if changes
            status = "submit"
            log_msg = ""
            # break if running jobs exceed
            if sent_jobs >= self.max_jobs - len_running:
                logger.debug("max jobs; break")
                break
            # get relevant data form atoms
            logger.debug("get runner data")
            runnerdata = RunnerData.from_data_dict(row.data.get("runner", None))
            try:
                (
                    scheduler_options,
                    name,
                    parents,
                    tasks,
                    files,
                ) = runnerdata.get_runner_data()
            except RuntimeError as err:
                logger.info("runner data corrupt/missing")
                # job failed if corrupt/missing runner data
                _ = row.data.get("runner", {})
                _.update(
                    {
                        "log": "{}\n{}\n".format(datetime.now(), err),
                        "fail_count": self.multi_fail + 1,
                    }
                )
                row.data["runner"] = _
                self.fdb.update(id_, status="failed", data=row.data)
                continue

            # add local runner things
            runner_data = self.pre_runner_data
            _ = runner_data.get_runner_data(_skip_empty_task_test=True)
            (pscheduler_options, _, _, ptasks, pfiles) = _
            scheduler_options.update(pscheduler_options)
            files.update(pfiles)
            tasks = ptasks + tasks  # prior execution of local tasks

            # get self and parents atoms object with everything
            logger.debug("getting atoms and parents")
            atoms = []
            try:
                atoms.append(
                    row.toatoms(attach_calculator=True, add_additional_information=True)
                )
            except AttributeError:
                atoms.append(
                    row.toatoms(
                        attach_calculator=False, add_additional_information=True
                    )
                )

            # if any parent is not done, then don't submit
            parents_done = True
            for i in parents:
                parent_row = self.fdb.get(i)
                if not parent_row.status == "done":
                    parents_done = False
                    break
                # !TODO: catch exception if user does not have permission
                # to read parent
                try:
                    _ = parent_row.toatoms(
                        attach_calculator=True, add_additional_information=True
                    )
                except AttributeError:
                    _ = parent_row.toatoms(
                        attach_calculator=False, add_additional_information=True
                    )
                parent = _
                atoms.append(parent)

            if not parents_done:
                logger.debug("parents pending")
                continue

            with Cd(self.run_folder):
                with Cd(str(id_)):
                    # submitting task
                    logger.debug("submitting {}".format(id_))

                    # preparing run script
                    (run_scripts, status, log_msg) = self._write_run_data(
                        atoms, tasks, files, status, log_msg
                    )
                    if status == "submit":
                        job_id, log_msg = self._submit(run_scripts, scheduler_options)
                        if job_id:
                            logger.debug("submitting success {}".format(job_id))
                            # update status and save job_id
                            status = "running"
                            with open("job.id", "w") as file_o:
                                file_o.write("{}".format(job_id))
                            sent_jobs += 1
                        else:
                            logger.debug("submitting failed {}".format(job_id))
                            status = "failed"

            # updating database
            data = row.data
            _ = data["runner"].get("log", "") + log_msg
            data["runner"]["log"] = _
            logger.debug("updating database")
            # adds status, name of calculation, and data
            self.fdb.update(id_, status=status, run_name=name, data=data)
            logger.info(
                "ID {} submission: {}".format(
                    id_, (status if status == "failed" else "successful")
                )
            )

    def _write_run_data(self, atoms, tasks, files, status, log_msg):
        """
        writes run data in the folder for excecution
        """
        # write files
        for i, string in files.items():
            if string.startswith("data:application/octet-stream;base64,"):
                write_mode = "wb"
                string = b64decode(string[37:].encode())
            else:
                write_mode = "w"
            with open(i, write_mode) as file_o:
                file_o.write(string)

        # write atoms
        with open("atoms.pkl", "wb") as file_o:
            pickle.dump(atoms, file_o)

        # copy run file
        shutil.copyfile(run.__file__, "run.py")

        # write run scripts
        run_scripts = []
        py_run = 0
        for task in tasks:
            if task[0] == "shell":
                # shell run
                shell_run = task[1]
                if isinstance(shell_run, list):
                    shell_run = " ".join(map(str, shell_run))
                run_scripts.append(shell_run)
            elif task[0] == "python":
                # python run
                shell_run = "python"
                if len(task) > 3:
                    shell_run = task[3]

                if len(task) > 2:
                    params = task[2]
                else:
                    params = {}

                # write params
                try:
                    with open("params{}.json".format(py_run), "w") as file_o:
                        json.dump(params, file_o)
                except TypeError as err:
                    status = "failed"
                    log_msg = "{}\n Error writing params: {}\n".format(
                        datetime.now(), err.args[0]
                    )
                    break
                # making python executable
                func_name = task[1]
                func_name = func_name[:-3] if func_name.endswith(".py") else func_name

                # add to run_scripts
                shell_run += f" run.py {func_name} {py_run} > run{py_run}.out"
                run_scripts.append(shell_run)
                py_run += 1

        return run_scripts, status, log_msg

    def _cancel_run(self):
        """
        Cancels run in cancel
        """
        cancel_ids = []
        for row in self.fdb.select(
            status="cancel", runner=f"{self.name}", columns=["id"], include_data=False
        ):
            cancel_ids.append(row.id)
        for id_ in cancel_ids:
            row = self.fdb.get(id_)
            logger.debug("cancel {}".format(id_))
            job_id = self.get_job_id(id_)
            if job_id:
                logger.debug("found {}".format(id_))
                # cancel the job and update database
                self._cancel(job_id)
                status, log_msg = [
                    "failed",
                    "{}\nCancelled by user\n".format(datetime.now()),
                ]
            else:
                logger.debug("lost {}".format(id_))
                # no job_id but still cancel, eg when pending
                status, log_msg = [
                    "failed",
                    "{}\nCancelled by user, no job was running\n".format(
                        datetime.now()
                    ),
                ]
            # updating status and log
            data = row.data
            _ = data["runner"].get("log", "") + log_msg
            data["runner"]["log"] = _
            logger.debug("update {}".format(id_))
            self.fdb.update(id_, status=status, data=data)
            logger.info("Cancelled {}".format(id_))

    def spool(self, _endless=True):
        """
        Does the spooling of jobs
        """
        # since the user is now spooling, the runner should update the
        # metadata of the database, this will raise error if the runner
        # is already running
        self.to_database(update=True)
        # now set the runner as running
        self._set_running()
        try:
            while True:
                # check for metadata stop
                # get present metadata
                with db.connect(self.database) as fdb:
                    meta = fdb.metadata
                if self.name in meta["runners"]:
                    if meta["runners"][self.name].get("_explicit_stop", False):
                        logger.info("Encountered stop in metadata.")
                        break
                else:
                    logger.info("Runner removed from metadata with force.")
                    break

                # starting operation
                logger.info("Searching failed jobs")
                failed_ids = []
                for row in self.fdb.select(
                    status="failed",
                    runner=f"{self.name}",
                    columns=["id"],
                    include_data=False,
                ):
                    failed_ids.append(row.id)
                for id_ in failed_ids:
                    row = self.fdb.get(id_)
                    update = False
                    if "runner" not in row.data:
                        row.data["runner"] = {}
                    if "fail_count" not in row.data["runner"]:
                        row.data["runner"]["fail_count"] = self.multi_fail + 1
                    if row.data["runner"]["fail_count"] <= self.multi_fail:
                        # submit in next cycle
                        logger.debug("re-submitted: {}".format(id_))
                        update = True
                    if update:
                        self.fdb.update(id_, status="submit", data=row.data)

                # cancel jobs
                logger.info("Cancelling jobs, if jobs to cancel")
                self._cancel_run()

                # update if running have finished
                logger.info("Updating running status")
                self._update_status_running()

                # send submit for run
                logger.info("Submitting")
                self._submit_run()

                if _endless:
                    # sleep before checking again
                    logger.info("Sleeping for {}s".format(self.cycle_time))
                    time.sleep(self.cycle_time)
                else:
                    # used for testing
                    break
        except KeyboardInterrupt:
            pass
        finally:
            self._unset_running()
