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


def get_graphical_status(filename, input_id, database, add_tasks=False):
    """Returns dot graph of the status of all parents of
    input_id

    Args:
        filename (str): name of file to store the graph (pdf, png or svg)
        input_id (int): input_id in the database
        database (str): the database name
        add_tasks (bool): adds tasks to the graph
    """
    try:
        from graphviz import Digraph
    except ModuleNotFoundError:
        raise ModuleNotFoundError('get_graphical_status needs graphviz, '
                                  'run: pip install graphviz')

    def get_info(input_id, database):
        """returns parent list"""
        parents = []
        tasks = []
        name = None
        with db.connect(database) as fdb:
            row = fdb.get(input_id)
            formula = row.formula
            status = row.get('status', 'No status')
            if 'runner' in row.data:
                parents = row.data['runner'].get('parents', [])
                tasks = row.data['runner']['tasks']
                name = row.data['runner']['name']
        return formula, name, parents, status, tasks

    def add_task_graph(name, tasks, dot):
        """adds task graph

        Args:
            name (str): name of runnerdata
            tasks (list): tasks list
            dot (dot): dot
        """
        param_task = []
        node_str = 'Tasks:'
        for i, task in enumerate(tasks):
            # add params if python params exist
            if task[0] == 'python':
                if task[2]:
                    # is not empty param
                    param_task.append(i)
            node_str += f'|<{i}>{task[1]}'
        node_str = f'{{{node_str}}}'  # making it vertical
        dot.node(f'{name}-tasks', node_str, shape='record')

        # adding param nodes
        for i in param_task:
            node_str = [f'{{{key}|{value}}}'
                        for key, value in tasks[i][2].items()]
            node_str = '|'.join(node_str)
            node_str = f'{{{node_str}}}'  # making it vertical
            dot.node(f'{name}-tasks-{i}', node_str,
                     shape='record')
            dot.edge(f'{name}-tasks-{i}', f'{name}-tasks:{i}')

    def spider(id_, dot):
        formula, name, parents, status, tasks = get_info(id_, database)
        dot.node(str(id_), label=f'{id_}: {formula}',
                 style='filled',
                 fillcolor='lightblue')

        if name:
            # add runner data graph
            # get unique node name
            node_name = f'{id_}-runnerdata'
            color = status_colors[status.split(':')[0]]
            dot.node(node_name, label=f'{name}\n{status}',
                     shape='box',
                     style='filled',
                     fillcolor=color)

            if add_tasks:
                add_task_graph(node_name, tasks, dot)
                dot.edge(f'{node_name}-tasks', node_name)

            # now connect it to the formula node
            dot.edge(node_name, str(id_))

            # add parents
            for parent in parents:
                spider(parent, dot)
                # connect the parent
                dot.edge(str(parent), node_name)

    status_colors = {'running': 'yellow',
                     'failed': 'red',
                     'submit': 'lightgreen',
                     'cancel': 'grey',
                     'done': 'green',
                     'No status': 'white'}
    dot = Digraph(comment='The Runner workflow', strict=True)
    spider(input_id, dot)
    fileformat = filename.split('.')[-1]
    filename = '.'.join(filename.split('.')[:-1])
    dot.render(filename, format=fileformat)
