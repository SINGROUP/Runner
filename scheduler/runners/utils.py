"""
Utility tools for schedulers
"""
from datetime import datetime
import os


run_py = """
import json
import pickle
from ase.atoms import Atoms
from {func} import main

def json_keys2int(x):
    # if dict key can be converted to int
    # then convert to int
    if isinstance(x, dict):
        try:
            return {{int(k):v for k,v in x.items()}}
        except ValueError:
            pass
    return x

with open("params{ind}.json") as f:
    params = json.load(f, object_hook=json_keys2int)
with open("atoms.pkl", "rb") as f:
    atoms = pickle.load(f)
atoms = main(atoms, {astr}params)
with open("atoms.pkl", "wb") as f:
    pickle.dump(atoms, f)
"""


def get_scheduler_data(data):
    """
    helper function to get complete scheduler data
    Example:
        {'scheduler_options': {'-N': 1,
                               '-n': 16,
                               '-t': '0:5:0:0',
                               '--mem-per-cpu': 2000},
         'name': '<calculation name>',
         'parents': [],
         'tasks': [['python', ['<filename>', <params>]], # python run
                   ['mpirun -n 4  python', ['<filename>', <params>]], #parallel
                   ['shell', '<command>']] # any shell command
         'files': {'<filename1>': '<contents, string or bytes>',
                   '<filename2>': '<contents, string or bytes>'
                  }
         'log': ''}
    Parameters
        data: dict
            scheduler data in atoms data
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
    success = True
    log = '{}\n'.format(datetime.now())
    scheduler_options = {}
    name = ''
    parents = []
    tasks = []
    files = {}

    if data is None:
        success = False
        log += 'No scheduler data\n'
    else:
        scheduler_options = data.get('scheduler_options', {})
        name = str(data.get('name', 'Calculation'))
        parents = data.get('parents', [])
        tasks = data.get('tasks', [])
        files = data.get('files', {})
        log_msg = data.get('log', '')
        log = log_msg + log

        if not isinstance(parents, (list, tuple)):
            success = False
            log += 'Scheduler: Parents should be a list of int\n'
        else:
            for i in parents:
                if isinstance(i, int):
                    log += 'Scheduler: parents should be a list of int\n'
                    success = False

        if not isinstance(files, dict):
            success = False
            log += 'Scheduler: files should be a dictionary\n'

        if not isinstance(tasks, (list, tuple)):
            success = False
            log += 'Scheduler: tasks should be a list\n'
        else:
            if len(tasks) == 0:
                # true if no tasks given on init
                # only an error if found during spooling
                log += "Scheduler: tasks empty"
            for i, task in enumerate(tasks):
                if str(task[0]) == 'shell' or str(task[0]).endswith('python'):
                    if task[0].endswith('python'):
                        if isinstance(task[1], str):
                            # add empty params
                            task[1] = [task[1], []]
                        elif not isinstance(task[1], (list, tuple)):
                            success = False
                            log += ('Scheduler: python task should be a list '
                                    'with file string and parameters\n')
                        elif len(task[1]) == 0 or len(task[1]) > 2:
                            success = False
                            log += ('Scheduler: python task should be a list '
                                    'with file string and parameters\n')
                        elif len(task[1]) == 1:
                            # add empty params
                            task[1] = [task[1], []]
                        elif not isinstance(task[1][1], (list, tuple, dict)):
                            success = False
                            log += ('Scheduler: python parameters should'
                                    ' be either list or dict')
                else:
                    success = False
                    log += ("Scheduler: task should either be 'shell' or "
                            "'<optional prefix> python'\n")

    return (scheduler_options, name, parents, tasks, files, success, log)


class Cd:
    """Context manager for changing the current working directory"""
    def __init__(self, new_path, mkdir=True):
        self.new_path = os.path.expanduser(new_path)
        self.saved_path = None

        if not os.path.exists(new_path) and mkdir:
            os.mkdir(new_path)

    def __enter__(self):
        self.saved_path = os.getcwd()
        os.chdir(self.new_path)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.saved_path)
