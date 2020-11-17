""" Utility to handle scheduler data"""
import json
from datetime import datetime
import ase.db as db
from runner.utils import json_keys2int
from copy import copy


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
             'log': ''}
        where <params> can be a dictionary of parameters,
                       or an empty {} for no parameters
              <py_commands> is a string of python command,
                            example, 'python3' or 'mpirun -n 4 python3'
                            default 'python'
        Parameters
            data: dict
                scheduler data in atoms data
        """
        self.data = data

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
        tasks = data.get('tasks', [])
        files = data.get('files', {})
        log_msg = data.get('log', '')

        if not isinstance(parents, (list, tuple)):
            err = log_msg + 'Runner: Parents should be a list of int\n'
            raise RuntimeError(err)
        else:
            for i in parents:
                if not isinstance(i, int):
                    err = (log_msg + 'Runner: parents should be a list of'
                           'int\n')
                    raise RuntimeError(err)
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

        if not isinstance(tasks, (list, tuple)):
            err = (log_msg + 'Runner: tasks should be a list\n')
            raise RuntimeError(err)
        else:
            if len(tasks) == 0:
                if not _skip_empty_task_test:
                    err = log_msg + "Runner: tasks empty\n"
                    raise RuntimeError(err)
            for i, task in enumerate(tasks):
                if not isinstance(task, (tuple, list)):
                    err = (log_msg + 'Runner: each task should be a list\n')
                    raise RuntimeError(err)
                if len(task) < 2:
                    err = (log_msg + 'Runner: each task sould have a name'
                           ' and command or filename\n')
                    raise RuntimeError(err)
                if str(task[0]) == 'shell':
                    if not isinstance(task[1], str):
                        err = (log_msg + 'Runner: shell command should'
                               'be str\n')
                        raise RuntimeError(err)
                elif task[0] == 'python':
                    if not isinstance(task[1], str):
                        err = (log_msg + 'Runner: python filename should be'
                               ' str\n')
                        raise RuntimeError(err)
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
                else:
                    raise RuntimeError("Runner: task should either be 'shell'"
                                       " or 'python'\n")

        return (scheduler_options, name, parents, tasks, files)

    def to_db(self, databse, id_):
        """add run data to id_ in database
        Parameters:
            databse: ase database
            id_: id in the database"""
        # test if data is appropriate
        (_, _, _, _, _, success, log) = self.get_runner_data()
        if not success:
            raise RuntimeError(log)
        with db.connect(databse) as fdb:
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
            return cls(fdb.get(id_).data['runner'])

    @classmethod
    def from_json(cls, filename):
        """get RunnerData from json"""
        with open(filename) as fio:
            data = json.load(fio, object_hook=json_keys2int)
        return cls(data)
