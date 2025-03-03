"""Utility to handle runner data"""

import json
import os
from base64 import b64encode
from copy import copy

from runner.utils.utils import get_db_connect, json_keys2int

default_files = ["run.sh", "batch.slrm", "atoms.pkl", "run.py", "status.txt", "job.id"]


class RunnerData:
    """Class to handle runner data using helper function

    Example:

      >>> # typical runner data
      >>> data =  {'scheduler_options': {'-N': 1,
      ...                                '-n': 16,
      ...                                '-t': '0:5:0:0',
      ...                                '--mem-per-cpu': 2000},
      ...          'name': '<calculation name>',
      ...          'parents': [],
      ...          'tasks': [['python', '<filename>'], # simple python run
      ...                    ['python', '<filename>', <params>],
      ...                    ['python', '<filename>', <params>, '<pycommand>'],
      ...                    ['shell', '<command>']] # any shell command
      ...          'files': {'<filename1>': '<contents, string or bytes>',
      ...                    '<filename2>': '<contents, string or bytes>'
      ...                   }
      ...          'keep_run': False
      ...          'log': ''}
      >>> runnerdata = RunnerData.from_data_dict(data)

      where:

        * <params>: can be a dictionary of parameters,
          or an empty {} for no parameters
        * <pycommand>: is a string of python command,
          example, 'python3' or 'mpirun -n 4 python3'
          default 'python'
        * keep_run: is a bool to keep run after status done, otherwise
          the run folder is deleted.

      However, the :class:`RunnerData` can be used to generate the data
      stepwise, using the functions provided as::

        >>> runnerdata = RunnerData('<calculation name>')
        >>> runnerdata.add_file('<filename>')
        >>> runnerdata.append_tasks('python',
        ...                         '<filename>',
        ...                         params_dict,
        ...                         '<pycommand>')
        >>> runnerdata.add_scheduler_options({'-N': 1,
        ...                                   '-n': 16,
        ...                                   '-t': '0:5:0:0',
        ...                                   '--mem-per-cpu': 2000})
        >>> # and so on

    Args:
        name (str): name of RunnerData

    Attributes:
        data: dictionary of the runner data
    """

    def __init__(self, name="untitled_run"):
        self.data = {
            "scheduler_options": {},
            "name": name,
            "tasks": [],
            "files": {},
            "parents": [],
            "keep_run": False,
        }

    def __repr__(self):
        return repr(self.data)

    @property
    def name(self):
        """Name of the RunnerData"""
        return self.data["name"]

    @name.setter
    def name(self, name):
        _test_name(name)
        self.data["name"] = name

    @property
    def tasks(self):
        """tasks in RunnerData"""
        return self.data["tasks"]

    @tasks.setter
    def tasks(self, tasks):
        _test_tasks(tasks, self.files, _skip_empty_task_test=True)
        self.data["tasks"] = tasks

    def append_tasks(self, task_type, *args):
        """Appends task to tasks

        Example:
            >>> rdat = runner.RunnerData()
            >>> # shell task_type followed by shell command
            >>> rdat.append_tasks('shell', 'module load anaconda3')
            >>> # python task_type followed by python file
            >>> rdat.append_tasks('python', 'get_energy.py')
            >>> # python task_type with parameters
            >>> rdat.append_tasks('python', 'get_energy.py', {'param': 0})
            >>> # python task_type with python execute command
            >>> # NB: the 3rd argument has to be parameters, if no parameters
            >>> # empty dict has to be given.
            >>> # default: python <python file>
            >>> # to execute: mpirun -n 4 python3 get_energy.py
            >>> rdat.append_tasks('python', 'get_energy.py', {},
            ...                   'mpirun -n 4 python3')


        Args:
            task_type (str): task type, 'shell' or 'python'
            *args: args for task type, see example
                for shell task_type, args is shell command (str)
                for python task_type, args is python filename (str),
                parameters (dict), and python execute command (str)
        """
        if task_type == "shell":
            task = ["shell", *args]
        elif task_type == "python":
            task = ["python", *args]
            if len(task) == 2:
                task.append({})
            if len(task) == 3:
                task.append("python")
        else:
            raise RuntimeError("task type shell or python supported")

        _test_tasks([task], self.files)
        self.data["tasks"].append(task)

    @property
    def files(self):
        """Files in RunnerData"""
        return self.data["files"]

    @files.setter
    def files(self, files):
        _test_files(files)
        self.data["files"] = files

    def add_file(self, filename, add_as=None):
        """Add file to runner data

        Args:
            filename (str): name of the file
            add_as (str): name the file should be added as"""
        if add_as is None:
            add_as = filename
        try:
            with open(filename, "r") as fio:
                basename = os.path.basename(add_as)
                self.data["files"][basename] = fio.read()
        except UnicodeDecodeError:
            # file is binary
            with open(filename, "rb") as fio:
                basename = os.path.basename(add_as)
                self.data["files"][basename] = (
                    "data:application/octet-stream;base64,"
                    + b64encode(fio.read()).decode()
                )

    def add_files(self, filenames, add_as=None):
        """Adds files to runner data

        Args:
            filenames (list): list of filenames to be added
            add_as (list, optional): list of name the file should be
                                     added as in the runner data"""
        if not isinstance(filenames, (tuple, list)):
            filenames = [filenames]
        if add_as is not None:
            if not isinstance(add_as, (tuple, list)):
                add_as = [add_as]
            if len(add_as) != len(filenames):
                raise RuntimeError("Length of filenames and add_as should be the same")
        else:
            add_as = filenames

        for name_, filename in zip(add_as, filenames):
            self.add_file(filename, name_)

    @property
    def scheduler_options(self):
        """Scheduler_options in RunnerData"""
        return self.data["scheduler_options"]

    @scheduler_options.setter
    def scheduler_options(self, scheduler_options):
        _test_scheduler_options(scheduler_options)
        self.data["scheduler_options"] = scheduler_options

    def add_scheduler_options(self, scheduler_options):
        """Adds scheduler_options to runner data

        Args:
            scheduler_options (dict): dictionary of options"""
        _test_scheduler_options(scheduler_options)
        self.data["scheduler_options"].update(scheduler_options)

    @property
    def parents(self):
        """Parent simulations of the row"""
        return self.data["parents"]

    @parents.setter
    def parents(self, parents):
        """set parents to runner data"""
        _test_parents(parents)
        self.data["parents"] = parents

    @property
    def keep_run(self):
        """Stores bool, indicates if the run should be saved after completing
        tasks

        .. note::
            Failed run folders are not deleted regardless of keep_run value.
            This aids in the debugging of the run."""
        return self.data["keep_run"]

    @keep_run.setter
    def keep_run(self, keep_run):
        _test_keep_run(keep_run)
        self.data["keep_run"] = keep_run

    def get_runner_data(self, _skip_empty_task_test=False):
        """
        helper function to get complete runner data

        Returns:
            dict: containing all options to run a job
            str: name of the calculation, for tags
            list: list of parents attached to the present job
            list: list of tasks to perform
            dict: dictionary of filenames as key and strings as value
        """
        data = self.data
        scheduler_options = {}
        name = ""
        parents = []
        tasks = []
        files = {}

        if data is None:
            raise RuntimeError("No runner data")

        scheduler_options = data.get("scheduler_options", {})
        name = str(data.get("name", "untitled_run"))
        parents = data.get("parents", [])
        files = data.get("files", {})
        tasks = data.get("tasks", [])
        keep_run = data.get("keep_run", False)
        log_msg = data.get("log", "")

        _test_scheduler_options(scheduler_options, log_msg)
        _test_name(name, log_msg)
        _test_parents(parents, log_msg)
        _test_files(files, log_msg)
        _test_tasks(tasks, files, log_msg, _skip_empty_task_test)
        _test_keep_run(keep_run, log_msg)

        return (scheduler_options, name, parents, tasks, files)

    def to_db(self, database, ids):
        """add run data to ids in database

        Args:
            database (str): ase database
            ids (int, or list): ids in the database"""
        if not isinstance(ids, (tuple, list)):
            ids = [ids]
        # test if data is appropriate
        _ = self.get_runner_data()
        fdb = get_db_connect(database)
        for id_ in ids:
            data = fdb.get(id_).data
            data["runner"] = self.data
            fdb.update(id_, data=data)

    def to_json(self, filename):
        """Saves RunnerData to json

        Args:
            filename (str): name of `json` file"""
        with open(filename, "w") as fio:
            json.dump(self.data, fio)

    @classmethod
    def from_db(cls, database, id_):
        """get RunnerData from database

        Args:
            databse (str): ase database
            id_ (int): id in the database

        Returns:
            :class:`~runner.utils.runnerdata.RunnerData`: class defining
            runner data
        """
        fdb = get_db_connect(database)
        data = fdb.get(id_).data["runner"]
        data.pop("log", None)
        return cls.from_data_dict(data)

    @classmethod
    def from_json(cls, filename):
        """get RunnerData from json

        Args:
            filename (str): name of `json` file

        Returns:
            :class:`~runner.utils.runnerdata.RunnerData`: class defining
            runner data
        """
        with open(filename) as fio:
            data = json.load(fio, object_hook=json_keys2int)
        return cls.from_data_dict(data)

    @classmethod
    def from_data_dict(cls, data):
        """Construct RunnerData from data dictionary

        Args:
            data (dict): runnerdata dictionary

        Returns:
            :class:`~runner.utils.runnerdata.RunnerData`: class defining
            runner data
        """
        runnerdata = cls()
        if data:
            runnerdata.data.update(data)

        return runnerdata


def _test_name(name, log_msg=""):
    if not isinstance(name, str):
        err = log_msg + "Runner: name should be str\n"
        raise RuntimeError(err)


def _test_keep_run(keep_run, log_msg=""):
    if not isinstance(keep_run, bool):
        err = log_msg + "Runner: keep_run should be bool\n"
        raise RuntimeError(err)


def _test_parents(parents, log_msg=""):
    if not isinstance(parents, (list, tuple)):
        err = log_msg + "Runner: Parents should be a list of int\n"
        raise RuntimeError(err)
    for i in parents:
        if not isinstance(i, int):
            err = log_msg + "Runner: parents should be a list ofint\n"
            raise RuntimeError(err)


def _test_files(files, log_msg=""):
    if not isinstance(files, dict):
        err = log_msg + "Runner: files should be a dictionary\n"
        raise RuntimeError(err)
    for filename, content in files.items():
        if filename in default_files:
            raise RuntimeError(log_msg + f"Runner: {filename=} in {default_files=}")
        if not isinstance(filename, str):
            err = log_msg + "Runner: filenames should be str\n"
            raise RuntimeError(err)
        if not isinstance(content, (str, bytes)):
            err = log_msg + "Runner: file contents should be str or bytes\n"
            raise RuntimeError(err)


def _test_tasks(tasks, files=None, log_msg="", _skip_empty_task_test=True):
    if files is None:
        files = {}
    if not isinstance(tasks, (list, tuple)):
        err = log_msg + "Runner: tasks should be a list\n"
        raise RuntimeError(err)

    if len(tasks) == 0 and not _skip_empty_task_test:
        err = log_msg + "Runner: tasks empty\n"
        raise RuntimeError(err)
    for task in tasks:
        if not isinstance(task, (tuple, list)):
            err = log_msg + "Runner: each task should be a list\n"
            raise RuntimeError(err)
        if len(task) < 2:
            err = (
                log_msg
                + "Runner: each task sould have a name and command or filename\n"
            )
            raise RuntimeError(err)
        if not isinstance(task[1], str):
            err = log_msg + "Runner: shell command or python filename should be str\n"
            raise RuntimeError(err)
        if task[0] == "python":
            # testing filename in files
            filename = copy(task[1])
            if not filename.endswith(".py"):
                filename += ".py"
            if filename not in files:
                err = (
                    log_msg
                    + "Runner: python filename {} should be in files\n".format(filename)
                )
                raise RuntimeError(err)
            if len(task) > 2:
                if not isinstance(task[2], dict):
                    err = log_msg + "Runner: python parameters should be dict\n"
                    raise RuntimeError(err)
            if len(task) > 3:
                if not isinstance(task[3], str):
                    err = log_msg + "Runner: python command shouldbe str\n"
                    raise RuntimeError(err)
        elif task[0] != "shell":
            raise RuntimeError("Runner: task should either be 'shell' or 'python'\n")


def _test_scheduler_options(scheduler_options, log_msg=""):
    if not isinstance(scheduler_options, dict):
        err = log_msg + "Runner: scheduler_options should be a dict\n"
        raise RuntimeError(err)


def _tasks2file(tasks):
    """converts tasks to run_scripts and files"""
    pass
