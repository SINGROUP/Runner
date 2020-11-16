""" Utility to handle scheduler data"""
import json
from datetime import datetime
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
                       ['python', ['<filename>', <params>],
                       ['python', ['<filename>', <params>, '<py_commands>']],
                       ['shell', '<command>']] # any shell command
             'files': {'<filename1>': '<contents, string or bytes>',
                       '<filename2>': '<contents, string or bytes>'
                      }
             'log': ''}
        where <params> can be a list or dictionary of parameters,
                       or an empty list for no parameters
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
            sucess: bool
                successfully read runner data
            log: str
                log of failing to read runner data
        """
        data = self.data
        success = True
        log = '{}\n'.format(datetime.now())
        scheduler_options = {}
        name = ''
        parents = []
        tasks = []
        files = {}

        if data is None:
            success = False
            log += 'No runner data\n'
            return [None] * 5 + [success, log]

        scheduler_options = data.get('scheduler_options', {})
        name = str(data.get('name', 'untitled_run'))
        parents = data.get('parents', [])
        tasks = data.get('tasks', [])
        files = data.get('files', {})
        log_msg = data.get('log', '')
        log = log_msg + log

        if not isinstance(parents, (list, tuple)):
            success = False
            log += 'Runner: Parents should be a list of int\n'
        else:
            for i in parents:
                if isinstance(i, int):
                    log += 'Runner: parents should be a list of int\n'
                    success = False

        if not isinstance(files, dict):
            success = False
            log += 'Runner: files should be a dictionary\n'

        if not isinstance(tasks, (list, tuple)):
            success = False
            log += 'Runner: tasks should be a list\n'
        else:
            if len(tasks) == 0:
                if not _skip_empty_task_test:
                    success = False
                log += "Runner: tasks empty"
            for i, task in enumerate(tasks):
                if str(task[0]) == 'shell':
                    if not isinstance(task[1], str):
                        success = False
                        log += ('Runner: shell command should'
                                'be str')
                elif task[0] == 'python':
                    if isinstance(task[1], str):
                        # add empty params
                        task[1] = [task[1], []]
                    elif not isinstance(task[1], (list, tuple)):
                        success = False
                        log += ('Runner: python task should be str'
                                'filename or a list with str filename'
                                ', parameters and optional'
                                'python command\n')
                    elif len(task[1]) == 0 or len(task[1]) >= 2:
                        success = False
                        log += ('Runner: python task should be str'
                                'filename or a list with str filename'
                                ', parameters and optional'
                                'python command\n')
                    elif len(task[1]) == 1:
                        # add empty params
                        task[1] = [task[1], []]
                    if not isinstance(task[1][1], (list, tuple, dict)):
                        success = False
                        log += ('Runner: python parameters should'
                                ' be either list or dict')
                    elif len(task[1]) == 3:
                        if not isinstance(task[1][2], str):
                            success = False
                            log += ('Runner: python command should'
                                    'be str')
                else:
                    success = False
                    log += ("Runner: task should either be 'shell' or "
                            "'python'\n")

        return (scheduler_options, name, parents, tasks, files, success, log)

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
