""" Utility to handle scheduler data"""
import json
import os
from copy import copy

import ase.db as db

from runner.utils import json_keys2int


class RunnerData():
    """Class to handle runner data"""

    def __init__(self, data=None):
        """
        helper function to get complete scheduler data
        Example:
            {'scheduler_options': {'-N': 1,
                                   '-n': 16,
                                   '-t': '0:5:0:0',
                                   '--mem-per-cpu': 2000},
             'name': '<calculation name>',
             'parents': [],
             'tasks': [['python', '<filename>'], # simple python run
                       ['python', '<filename>', <params>],
                       ['python', '<filename>', <params>, '<py_commands>'],
                       ['shell', '<command>']] # any shell command
             'files': {'<filename1>': '<contents, string or bytes>',
                       '<filename2>': '<contents, string or bytes>'
                      }
             'keep_run': False
             'log': ''}
        where <params> can be a dictionary of parameters,
                       or an empty {} for no parameters
              <py_commands> is a string of python command,
                            example, 'python3' or 'mpirun -n 4 python3'
                            default 'python'
              keep_run is a bool to keep run after status done, otherwise
                       the run folder is deleted.
        Parameters
            data: dict
                scheduler data in atoms data
        """
        data_temp = {'scheduler_options': {},
                     'name': 'untitled_run',
                     'tasks': [],
                     'files': {},
                     'keep_run': False}
        if data:
            data_temp.update(data)
        self.data = data_temp

    def __repr__(self):
        return repr(self.data)

    @property
    def name(self):
        """getter for name"""
        return self.data['name']

    @name.setter
    def name(self, name):
        _test_name(name)
        self.data['name'] = name

    @property
    def tasks(self):
        """getter for tasks"""
        return self.data['tasks']

    @tasks.setter
    def tasks(self, tasks):
        _test_tasks(tasks, self.files, _skip_empty_task_test=True)
        self.data['tasks'] = tasks

    def append_tasks(self, task_type, *args):
        """Appends task to tasks
        Example:
            >>> # shell task_type followed by shell command
            >>> self.append_tasks('shell', 'module load anaconda3')
            >>> # python task_type followed by python file
            >>> self.append_tasks('python', 'get_energy.py')
            >>> # python task_type with parameters
            >>> self.append_tasks('python', 'get_energy.py', {'param': 0})
            >>> # python task_type with python execute command
            >>> # NB: the 3rd argument has to be parameters, if no parameters
            >>> # empty dict has to be given.
            >>> # default: python <python file>
            >>> # to execute: mpirun -n 4 python3 get_energy.py
            >>> self.append_tasks('python', 'get_energy.py', {},
                                  'mpirun -n 4 python3')


        Parameters:
            task_type (str): task type, 'shell' or 'python'
            *args: args for task type, see example
                for shell task_type, args is shell command (str)
                for python task_type, args is python filename (str),
                parameters (dict), and python execute command (str)
        """
        if task_type == 'shell':
            task = ['shell', *args]
        elif task_type == 'python':
            task = ['python', *args]
            if len(task) == 2:
                task.append({})
            if len(task) == 3:
                task.append('python')
        else:
            raise RuntimeError('task type shell or python supported')

        _test_tasks([task], self.files)
        self.data['tasks'].append(task)

    @property
    def files(self):
        """getter for files"""
        return self.data['files']

    @files.setter
    def files(self, files):
        _test_files(files)
        self.data['files'] = files

    def add_file(self, filename, add_as=None):
        """Add file to runner data
        Args:
            filename (str): name of the file
            add_as (str): name the file should be added as"""
        if add_as is None:
            add_as = filename
        try:
            with open(filename, 'r') as fio:
                basename = os.path.basename(add_as)
                self.data['files'][basename] = fio.read()
        except UnicodeDecodeError:
            # file is binary
            with open(filename, 'rb') as fio:
                basename = os.path.basename(add_as)
                self.data['files'][basename] = fio.read()

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
                raise RuntimeError('Length of filenames and add_as should'
                                   ' be the same')
        else:
            add_as = filenames

        for name_, filename in zip(add_as, filenames):
            self.add_file(filename, name_)

    @property
    def scheduler_options(self):
        """getter for scheduler_options"""
        return self.data['scheduler_options']

    @scheduler_options.setter
    def scheduler_options(self, scheduler_options):
        _test_scheduler_options(scheduler_options)
        self.data['scheduler_options'] = scheduler_options

    def add_scheduler_options(self, scheduler_options):
        """Adds scheduler_options to runner data"""
        _test_scheduler_options(scheduler_options)
        self.data['scheduler_options'].update(scheduler_options)

    @property
    def parents(self):
        """getter for parents"""
        return self.data['parents']

    @parents.setter
    def parents(self, parents):
        """set parents to runner data"""
        _test_parents(parents)
        self.data['parents'] = parents

    @property
    def keep_run(self):
        """getter for keep_run"""
        return self.data['keep_run']

    @keep_run.setter
    def keep_run(self, keep_run):
        _test_keep_run(keep_run)
        self.data['keep_run'] = keep_run

    def get_runner_data(self, _skip_empty_task_test=False):
        """
        helper function to get complete runner data
        Returns
            scheduler_options: dict
                containing all options to run a job
            name: str
                name of the calculation, for tags
            parents: list
                list of parents attached to the present job
            tasks: list
                list of tasks to perform
            files: dict
                dictionary of filenames as key and strings as value
        """
        data = self.data
        scheduler_options = {}
        name = ''
        parents = []
        tasks = []
        files = {}

        if data is None:
            raise RuntimeError('No runner data')

        scheduler_options = data.get('scheduler_options', {})
        name = str(data.get('name', 'untitled_run'))
        parents = data.get('parents', [])
        files = data.get('files', {})
        tasks = data.get('tasks', [])
        keep_run = data.get('keep_run', False)
        log_msg = data.get('log', '')

        _test_scheduler_options(scheduler_options, log_msg)
        _test_name(name, log_msg)
        _test_parents(parents, log_msg)
        _test_files(files, log_msg)
        _test_tasks(tasks, files, log_msg, _skip_empty_task_test)
        _test_keep_run(keep_run, log_msg)

        return (scheduler_options, name, parents, tasks, files)

    def to_db(self, database, ids):
        """add run data to ids in database
        Parameters:
            databse: ase database
            ids: ids in the database"""
        if not isinstance(ids, (tuple, list)):
            ids = [ids]
        # test if data is appropriate
        _ = self.get_runner_data()
        for id_ in ids:
            with db.connect(database) as fdb:
                data = fdb.get(id_).data
                data['runner'] = self.data
                fdb.update(id_, data=data)

    def to_json(self, filename):
        """Saves RunnerData to json"""
        with open(filename, 'w') as fio:
            json.dump(self.data, fio)

    @classmethod
    def from_db(cls, database, id_):
        """get RunnerData from database
        Parameters:
            databse: ase database
            id_: id in the database
        """
        with db.connect(database) as fdb:
            data = fdb.get(id_).data['runner']
            data.pop('log', None)
            return cls(data)

    @classmethod
    def from_json(cls, filename):
        """get RunnerData from json"""
        with open(filename) as fio:
            data = json.load(fio, object_hook=json_keys2int)
        return cls(data)


def _test_name(name, log_msg=''):
    if not isinstance(name, str):
        err = log_msg + 'Runner: name should be str\n'
        raise RuntimeError(err)


def _test_keep_run(keep_run, log_msg=''):
    if not isinstance(keep_run, bool):
        err = log_msg + 'Runner: keep_run should be bool\n'
        raise RuntimeError(err)


def _test_parents(parents, log_msg=''):
    if not isinstance(parents, (list, tuple)):
        err = log_msg + 'Runner: Parents should be a list of int\n'
        raise RuntimeError(err)
    for i in parents:
        if not isinstance(i, int):
            err = (log_msg + 'Runner: parents should be a list of'
                   'int\n')
            raise RuntimeError(err)


def _test_files(files, log_msg=''):
    if not isinstance(files, dict):
        err = (log_msg + 'Runner: files should be a dictionary\n')
        raise RuntimeError(err)
    for filename, content in files.items():
        if not isinstance(filename, str):
            err = (log_msg + 'Runner: filenames should be str\n')
            raise RuntimeError(err)
        if not isinstance(content, (str, bytes)):
            err = (log_msg + 'Runner: file contents should be str'
                   ' or bytes\n')
            raise RuntimeError(err)


def _test_tasks(tasks, files=None, log_msg='',
                _skip_empty_task_test=True):
    if files is None:
        files = {}
    if not isinstance(tasks, (list, tuple)):
        err = (log_msg + 'Runner: tasks should be a list\n')
        raise RuntimeError(err)

    if len(tasks) == 0 and not _skip_empty_task_test:
        err = log_msg + "Runner: tasks empty\n"
        raise RuntimeError(err)
    for task in tasks:
        if not isinstance(task, (tuple, list)):
            err = (log_msg + 'Runner: each task should be a list\n')
            raise RuntimeError(err)
        if len(task) < 2:
            err = (log_msg + 'Runner: each task sould have a name'
                   ' and command or filename\n')
            raise RuntimeError(err)
        if not isinstance(task[1], str):
            err = (log_msg + 'Runner: shell command or python filename'
                   ' should be str\n')
            raise RuntimeError(err)
        if task[0] == 'python':
            # testing filename in files
            filename = copy(task[1])
            if not filename.endswith('.py'):
                filename += '.py'
            if filename not in files:
                err = (log_msg + 'Runner: python filename {} should'
                       ' be in files\n'.format(filename))
                raise RuntimeError(err)
            if len(task) > 2:
                if not isinstance(task[2], dict):
                    err = (log_msg + 'Runner: python parameters '
                           'should be dict\n')
                    raise RuntimeError(err)
            if len(task) > 3:
                if not isinstance(task[3], str):
                    err = (log_msg + 'Runner: python command should'
                           'be str\n')
                    raise RuntimeError(err)
        elif task[0] != 'shell':
            raise RuntimeError("Runner: task should either be 'shell'"
                               " or 'python'\n")


def _test_scheduler_options(scheduler_options, log_msg=''):
    if not isinstance(scheduler_options, dict):
        err = log_msg + 'Runner: scheduler_options should be a dict\n'
        raise RuntimeError(err)

def _tasks2file(tasks):
    """converts tasks to run_scripts and files"""
