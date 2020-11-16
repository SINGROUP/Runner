"""
Utility tools for schedulers
"""
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
atoms = main(atoms, **params)
with open("atoms.pkl", "wb") as f:
    pickle.dump(atoms, f)
"""


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


def json_keys2int(x):
    """ Converts dict keys to int if all dict keys can be converted to int
    JSON only has string keys, its a compromise to save int keys, if all int
    """
    if isinstance(x, dict):
        try:
            return {{int(k):v for k,v in x.items()}}
        except ValueError:
            pass
    return x

