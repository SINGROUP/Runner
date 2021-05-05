"""
Relay to form complex workflows
"""
import time
import json
from copy import deepcopy
import numpy as np
from ase import Atoms
from ase import db
from runner.utils import submit
from runner.utils.runnerdata import RunnerData
from runner.runners import runner_types


class Relay():
    """
    Relay :class:`~ase.db` rows into a workflow

    Args:
        label (str): unique label of relay, for easy access in graph
        parents (list): list of parents as input to the relay. Parents can be
            :class:`~ase.Atoms` object, id in the database, or :class:`~Relay`.
        runnerdata (:class:`~runner.RunnerData`): the runner data to be
            attached to the row.
        runnername (str): name of the runner that handles the row run.

        NB: In the case of :class:`~ase.Atoms` object as parent, the object can
            be the only one in the list.
    """

    def __init__(self, label, parents=None, runnerdata=None, runnername=None):
        # setting up defaults
        if parents is None:
            parents = []
        if runnerdata is None:
            runnerdata = RunnerData()

        # if relay commited to the database
        # None implies not commited
        self._database = None
        # if database needs update when relay is edited
        self._updated = False
        self.parents = parents
        self.runnerdata = deepcopy(runnerdata)
        self.runnername = runnername
        self.label = label

        # assigning arbitrary str id to reference before commiting
        letters = np.array(list(chr(ord('a') + i) for i in range(26)))
        used_ids = set()
        used_labels = set()
        for parent in self.parents:
            if isinstance(parent, Relay):
                used_ids |= set(parent._spider().keys())
                used_labels |= {value.label
                                for value in parent._spider().values()}
        used_labels -= set([''])
        if label in used_labels:
            raise RuntimeError(f'{label} already taken')

        while True:
            self.id_ = ''.join(np.random.choice(letters, 5))
            if self.id_ not in used_ids:
                break

    def __repr__(self):
        label_str = (f", label='{self.label}'" if self.label != '' else "")
        database_str = (f", database='{self._database}'"
                        if self._database is not None else "")
        repr_str = (f"{self.__class__.__name__}(id={self.id_}"
                    f", parents=[{', '.join([str(x) for x in self.parents])}]"
                    f"{database_str}"
                    f", runnerdata='{self._runnerdata.name}'"
                    f", runnername='{self._runnername}'"
                    f"{label_str})")
        return repr_str

    def __str__(self):
        label_str = (f", label='{self.label}'" if self.label != '' else "")
        return f"{self.__class__.__name__}(id={self.id_}{label_str})"

    def get_parent_relay(self, item):
        """
        Returns a Relay instance of one of the parents associated with item,
        in the parent relay

        Args:
            item (str or int): id or label associated with
        """
        spider = self._spider()
        ret_item = spider.get(item, None)
        if ret_item is None:
            ret_item = {(value.label if value.label != ''
                         else value.id_): value
                        for key, value in spider.items()}.get(item, None)
        return ret_item

    def list_parent_relay(self):
        """
        Returns a list of relay instances present in the relay.
        """
        spider = self._spider()
        list_relay = list(spider.keys())
        for value in spider.values():
            if value.label != '':
                list_relay.append(value.label)
        return list_relay

    def commit(self, database=None):
        """
        Commits the relay to the database, i.e. adds data to the database.

        Args:
            database (str): ASE database to commit to. If None (default) then
                stored database from previous commit is used.

        Returns:
            int: the id of the row commited to in the database.

        """
        if database is None:
            if self._database is None:
                raise RuntimeError(f'{self.__str__()} needs a'
                                   f' database for commit. No previous'
                                   f' database commits.')
            database = self._database
        elif self._database is not None:
            if self._database != database:
                raise RuntimeError(f'{self.__str__()} already commited with'
                                   f' {self._database}, cannot commit with'
                                   f' {database}')

        # recursively commit parents
        for parent in self.parents:
            if isinstance(parent, Relay):
                parent.commit(database=database)

        if self._updated:
            return self.id_

        if self._database is not None:
            # if commited, then check status
            with db.connect(database) as fdb:
                status = fdb.get(self.id_).get('status', '')

            if (status == 'submit'
                    or status == 'running'
                    or status == 'cancel'):
                raise RuntimeError(f'cannot commit {self.__str__()}. It is '
                                   f'either submitted, running, or being'
                                   f' cancelled.')

        with db.connect(database) as fdb:
            parents = self.parents
            parent_id = [parent.id_ if isinstance(parent, Relay) else parent
                         for parent in parents]
            if self._database is None:
                if len(parents) != 0 and isinstance(parents[0], Atoms):
                    self.id_ = fdb.write(parents[0])
                    self.runnerdata.parents = parent_id[1:]
                    self._parents.pop(0)
                else:
                    self.id_ = fdb.write(Atoms())
                    self.runnerdata.parents = parent_id
            elif len(parents) != 0 and isinstance(parents[0], Atoms):
                fdb.update(self.id_, parents[0])
                self.runnerdata.parents = parent_id[1:]
                self._parents.pop(0)
        self._database = database

        self.runnerdata.to_db(self.database, self.id_)
        self._updated = True

        return self.id_

    def needs_commit(self):
        """
        Checks all parents to confirm if relay needs a commit

        Returns:
            bool: if relay needs a commit
        """
        bool_list = [not self._updated]

        dict_ = self._spider()
        bool_list += [not parent._updated for parent in dict_.values()]

        return np.any(bool_list)

    def start(self, force=False):
        """
        Submits the relay for run. Rows with submitted parents are also
        submitted.

        NB: rows with status 'submit' or 'running' are not
            resubmitted.

        Args:
            force (bool): resubmit runs with 'done' status too.

        Returns:
            bool: if a row is submitted
        """

        if self.needs_commit():
            raise RuntimeError('Commit the relay.')

        # recursively submitting parents
        parent_submitted = False
        for parent in self.parents:
            if isinstance(parent, Relay):
                status = parent.start(force=force)
                parent_submitted = (parent_submitted
                                    or status)

        with db.connect(self.database) as fdb:
            status = fdb.get(self.id_).get('status', '')

        if status == 'submit' or status == 'running':
            return True
        if status == 'cancel':
            return False
        if (status == '' or status == 'failed'
                or (status == 'done'
                    and (force or parent_submitted))):
            submit(self.id_, self.database, self.runnername)
            return True
        return False

    @property
    def parents(self):
        """
        parents associated with the relay
        """
        return self._parents

    @parents.setter
    def parents(self, parents):
        if not isinstance(parents, list):
            parents = [parents]
        self._parents = parents
        self._updated = False

    @property
    def runnerdata(self):
        """
        runnerdata associated with the relay
        """
        return self._runnerdata

    @runnerdata.setter
    def runnerdata(self, runnerdata):
        assert isinstance(runnerdata, RunnerData)
        self._runnerdata = runnerdata
        self._updated = False

    @property
    def runnername(self):
        """
        runnername associated with the relay
        """
        return self._runnername

    @runnername.setter
    def runnername(self, runnername):
        if runnername is not None:
            assert isinstance(runnername, str)
            is_defined = runnername.split(':')[0] in runner_types
            assert is_defined, f'{runnername} type not in {runner_types}'
        self._runnername = deepcopy(runnername)
        self._updated = False

    @property
    def database(self):
        """
        database associated with the relay
        """
        return self._database

    @database.setter
    def database(self, database):
        raise RuntimeError('database should not be changed.')

    @property
    def label(self):
        """
        runnername associated with the relay
        """
        return self._label

    @label.setter
    def label(self, label):
        assert isinstance(label, str)
        self._label = label

    def get_row(self, cycle_time=10, wait=True):
        """
        Get atoms row attached with the relay. Waits for the run if not
        finished.

        Args:
            cycle_time (float): time (s) to cycle the query.
            wait (bool): wait for run to finish

        Returns:
            :class:`~ase.db.row`: atoms row of attached with the relay
        """
        if self._database is None:
            raise RuntimeError('Relay not commited')

        fdb = db.connect(self.database)

        while True:
            row = fdb.get(self.id_)

            status = row.get('status', 'done')

            if status == 'done' or not wait:
                break
            if status == 'failed':
                raise RuntimeError(f'Run {self.id_} failed')

            time.sleep(cycle_time)

        return row

    row = property(get_row, doc=('Atoms row attached with the relay, awaits '
                                 'for the run to finish'))

    def _to_dict(self):
        """
        Converts relay to dict. relay parents are saved as id.
        """
        dict_ = {}
        dict_['id'] = self.id_
        dict_['updated'] = self._updated
        dict_['database'] = self.database
        dict_['runnerdata'] = self.runnerdata.data
        dict_['runnername'] = self.runnername
        dict_['label'] = self.label
        dict_['parents'] = []
        for parent in self.parents:
            if isinstance(parent, Atoms):
                pass
                # dict_['parents'].append(parent.todict())
            elif isinstance(parent, int):
                dict_['parents'].append(parent)
            elif isinstance(parent, Relay):
                dict_['parents'].append(parent.id_)

        return dict_

    @classmethod
    def _from_dict(cls, dict_, parent_dict=None):
        """
        construct relay from relay dictionary

        Args:
            dict_ (dict): dictionary of relay, as output from Relay._to_dict()
            parent_dict (dict): a dictionary of ids as keys, relay instance as
                values. Used to construct parent graph of the relay.
        """

        # constructing parent
        if parent_dict is None:
            parent_dict = {}
        parents = []
        for parent in dict_['parents']:
            if isinstance(parent, (int, str)):
                if parent in parent_dict:
                    parents.append(parent_dict[parent])
                else:
                    parents.append(parent)
            elif isinstance(parent, dict):
                parents.append(Atoms.fromdict(parent))
            else:
                raise RuntimeError(f'Unidentified parent: {parent}')

        relay = cls(dict_['label'],
                    parents,
                    RunnerData.from_data_dict(dict_['runnerdata']),
                    dict_['runnername'])
        relay.id_ = dict_['id']
        relay._updated = dict_['updated']
        relay._database = dict_['database']

        return relay

    def to_json(self, filename):
        """
        Save the relay as a json file

        Args:
            filename (str): json filename
        """
        dict_ = self._to_dict()
        spider = self._spider()
        for key, value in spider.items():
            spider[key] = value._to_dict()
        dict_['parent_dict'] = spider

        with open(filename, 'w') as fio:
            json.dump(dict_, fio)

    @classmethod
    def from_json(cls, filename):
        """
        Get relay from json file.

        Args:
            filename (str): json filename
        """
        with open(filename) as fio:
            data = json.load(fio)

        # preparing parent_dict
        parent_dict_ = data.pop('parent_dict', {})
        # make relay objects
        parent_dict = {}
        for key, value in parent_dict_.items():
            # json stores int as str
            try:
                key = int(key)
            except ValueError:
                pass
            parent_dict[key] = cls._from_dict(value)
        # make the parents in relay object as relay
        for key, value in parent_dict.items():
            for i, parent in enumerate(value._parents):
                if parent in parent_dict:
                    value._parents[i] = parent_dict[parent]

        relay = cls._from_dict(data, parent_dict)
        return relay

    def _spider(self, dict_=None, parent_call=None):
        """
        crawls the relay graph to collate the parent data, for easy access of
        parent relay instances

        Args:
            dict_ (dict): present dictionary of added relays
            parent_call (list): list of relays in the present parent crawl,
                used to detect cyclic graph calls

        Returns:
            dict: a dictionary of ids as keys and relay instance as value
        """
        if dict_ is None:
            dict_ = {}
        if parent_call is None:
            parent_call = set()

        parent_call.add(self.id_)

        for parent in self._parents:
            if isinstance(parent, Relay):
                if parent.id_ not in dict_:
                    if parent.id_ in parent_call:
                        raise RuntimeError('Cyclic connection detected.'
                                           ' Abandon ship!')
                    dict_[parent.id_] = parent

                    dict_ = parent._spider(dict_, parent_call)

        parent_call.discard(self.id_)

        return dict_
