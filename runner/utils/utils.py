"""
Utility tools for runners
"""
import os
import ase.db as db


RUN_PY = """
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
atoms = main(atoms, **params)
with open("atoms.pkl", "wb") as f:
    pickle.dump(atoms, f)
"""


class Cd:
    """Context manager for changing the current working directory

    :meta private:"""
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


def json_keys2int(dict_):
    """ Converts dict keys to int if all dict keys can be converted to int
    JSON only has string keys, its a compromise to save int keys, if all int

    :meta private:
    """
    if isinstance(dict_, dict):
        try:
            return {{int(k): v for k, v in dict_.items()}}
        except ValueError:
            pass
    return dict_


def get_status(input_id, database):
    """Gets status of input_id in the database

    Args:
        input_id (int): input_id in the database
        database (str): the database name

    Returns:
        str: the status of the run
    """
    input_id = int(input_id)
    with db.connect(database) as fdb:
        status = fdb.get(input_id).get('status', 'No status')
    return status


def submit(input_id, database, runner_name):
    """Submits input_id in the database

    Args:
        input_id (int): input_id in the database
        database (str): the database name
        runner_name (str): name of the runner

    Returns:
        str: the status of the run
    """
    input_id = int(input_id)
    with db.connect(database) as fdb:
        fdb.update(input_id, status=f'submit:{runner_name}')


def cancel(input_id, database):
    """Cancel run of input_id in the database

    Args:
        input_id (int): input_id in the database
        database (str): the database name
    """
    input_id = int(input_id)
    with db.connect(database) as fdb:
        row = fdb.get(input_id)
        if 'status' in row:
            status = row.status.split(':')
            status[0] = 'cancel'
            fdb.update(input_id, status=':'.join(status))
