"""
Relay to form complex workflows
"""

import json
import time
from copy import deepcopy

import numpy as np
from ase import Atoms, db

from runner.runners import runner_types
from runner.utils import cancel, get_graphical_status, submit
from runner.utils.runnerdata import RunnerData


class Relay:
    """
    Relay :class:`~ase.db` rows into a workflow

    Args:
        label (str): unique label of relay, for easy access in graph
        parents (list): list of parents as input to the relay. Parents can be
            :class:`~ase.Atoms` object, id in the database, or :class:`~Relay`.
        runnerdata (:class:`~runner.RunnerData`): the runner data to be
            attached to the row.
        runnername (str): name of the runner that handles the row run.

    .. note::
        In the case of :class:`~ase.Atoms` object as parent, the object can
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
        self.runnerdata = runnerdata
        self.runnername = runnername
        self.label = label

        # assigning arbitrary str id to reference before commiting
        letters = np.array(list(chr(ord("a") + i) for i in range(26)))
        used_ids = set()
        used_labels = set()
        for parent in self.parents:
            if isinstance(parent, Relay):
                used_ids |= set(parent._spider().keys())
                used_labels |= {value.label for value in parent._spider().values()}
        used_labels -= set([""])
        if label in used_labels:
            raise RuntimeError(f"{label} already taken")

        while True:
            self.id_ = "".join(np.random.choice(letters, 5))
            if self.id_ not in used_ids:
                break

    def __repr__(self):
        label_str = f", label='{self.label}'" if self.label != "" else ""
        database_str = (
            f", database='{self._database}'" if self._database is not None else ""
        )
        repr_str = (
            f"{self.__class__.__name__}(id={self.id_}"
            f", parents=[{', '.join([str(x) for x in self.parents])}]"
            f"{database_str}"
            f", runnerdata='{self._runnerdata.name}'"
            f", runnername='{self._runnername}'"
            f"{label_str})"
        )
        return repr_str

    def __str__(self):
        label_str = f", label='{self.label}'" if self.label != "" else ""
        return f"{self.__class__.__name__}(id={self.id_}{label_str})"

    def get_parent_relay(self, item):
        """
        Returns a Relay instance of one of the parents associated with item,
        in the parent relay

        Args:
            item (str or int): id or label associated with parent relay
        """
        spider = self._spider()
        ret_item = spider.get(item, None)
        if ret_item is None:
            ret_item = {
                (value.label if value.label != "" else value.id_): value
                for key, value in spider.items()
            }.get(item, None)
        return ret_item

    def list_parent_relay(self):
        """
        Returns a list of relay instances present in the relay.
        """
        spider = self._spider()
        list_relay = list(spider.keys())
        for value in spider.values():
            if value.label != "":
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
                raise RuntimeError(
                    f"{self.__str__()} needs a database for commit. "
                    f"No previous database commits."
                )
            database = self._database
        elif self._database is not None:
            if self._database != database:
                raise RuntimeError(
                    f"{self.__str__()} already commited with {self._database}, "
                    f"cannot commit with {database}"
                )

        # recursively commit parents
        for parent in self.parents:
            if isinstance(parent, Relay):
                parent.commit(database=database)

        if self._updated:
            return self.id_

        if self._database is not None:
            # if commited, then check status
            with db.connect(database) as fdb:
                status = fdb.get(self.id_).get("status", "")

            if status in ["submit", "running", "cancel"]:
                raise RuntimeError(
                    f"cannot commit {self.__str__()}. It is either submitted, "
                    f"running, or being cancelled."
                )

        with db.connect(database) as fdb:
            parents = self.parents
            parent_id = [
                parent.id_ if isinstance(parent, Relay) else parent
                for parent in parents
            ]
            if self._database is None:
                if len(parents) != 0 and isinstance(parents[0], Atoms):
                    self.id_ = fdb.write(parents[0], label=self.label)
                    self.runnerdata.parents = parent_id[1:]
                    self._parents.pop(0)
                else:
                    self.id_ = fdb.write(Atoms(), label=self.label)
                    self.runnerdata.parents = parent_id
                # now add to self
                self._database = database
            elif len(parents) != 0 and isinstance(parents[0], Atoms):
                fdb.update(self.id_, parents[0], label=self.label)
                self.runnerdata.parents = parent_id[1:]
                self._parents.pop(0)
            else:
                # update label
                fdb.update(self.id_, label=self.label)
                self.runnerdata.parents = parent_id

        self.runnerdata.to_db(self.database, self.id_)
        self._updated = True

        return self.id_

    def needs_commit(self):
        """
        Checks all parents to confirm if relay needs a commit

        Returns:
            bool: if relay needs a commit

        .. note::
            set_property methods should be used to update the relay. This
            marks the relay needs commit. If the set_property methods aren't
            used, then relay will not commit the changes.
        """
        bool_list = [not self._updated]

        dict_ = self._spider()
        bool_list += [not parent._updated for parent in dict_.values()]

        return np.any(bool_list)

    def start(self, force=False, force_all=False):
        """
        Submits the relay for run. Rows with submitted parents are also
        submitted.

        NB: rows with status 'submit' or 'running' are not
            resubmitted.

        Args:
            force (bool): resubmit row with 'done' status too.
            force_all (bool): resubmit all parent rows with 'done' status too.

        Returns:
            bool: if a row is submitted
        """

        if self.needs_commit():
            raise RuntimeError("Commit the relay.")

        # recursively submitting parents
        parent_submitted = False
        for parent in self.parents:
            if isinstance(parent, Relay):
                status = parent.start(force=force_all, force_all=force_all)
                parent_submitted = parent_submitted or status

        with db.connect(self.database) as fdb:
            status = fdb.get(self.id_).get("status", "")

        if status in ["submit", "running"]:
            return True
        if status == "cancel":
            return False
        if (status == "done" and (force or parent_submitted)) or status in [
            "",
            "failed",
        ]:
            submit(self.id_, self.database, self.runnername)
            return True
        return False

    def cancel(self, cancel_all=False):
        """
        Cancel row job

        Args:
            all (bool): Cancel all runs in the parent relay too.
        """
        if self._database is None:
            raise RuntimeError("Relay not commited")

        with db.connect(self._database) as fdb:
            status = fdb.get(self.id_).get("status", "")
            if status in ["submit", "running"]:
                cancel(self.id_, self._database)

            if cancel_all:
                spider = self._spider()
                for value in spider.values():
                    status = fdb.get(value.id_).get("status", "")
                    if status in ["submit", "running"]:
                        value.cancel(cancel_all=cancel_all)

    def get_status(self):
        """
        Returns status of the present row

        Returns:
            str: status of present row
        """
        if self._database is None:
            return "Relay not commited"
        with db.connect(self._database) as fdb:
            status = fdb.get(self.id_).get("status", "No status")
        return status

    status = property(get_status, doc=("Returns status of present relay row"))

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
        if len(parents) != 0 and not isinstance(parents[0], Atoms):
            bool_ = [isinstance(parent, (int, Relay)) for parent in parents]
            assert np.all(bool_), "parent should be a relay or an int index"
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
        self._runnerdata = deepcopy(runnerdata)
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
            is_defined = runnername.split(":")[0] in runner_types
            assert is_defined, f"{runnername} type not in {runner_types}"
        self._runnername = deepcopy(runnername)
        self._updated = False

    @property
    def database(self):
        """
        database associated with the relay
        """
        if self._database is None:
            raise RuntimeError("Rely not commited, no database exists.")
        return self._database

    @database.setter
    def database(self, database):
        raise RuntimeError("database should not be changed.")

    @property
    def label(self):
        """
        runnername associated with the relay
        """
        return self._label

    @label.setter
    def label(self, label):
        assert isinstance(label, str)
        for i in [int, float]:
            try:
                _ = i(label)
                raise RuntimeError(
                    f"Label {label} is put in as string"
                    f" but can be interpreted as {i.__name__}!"
                    f" Not accepted by ASE database"
                )
            except ValueError:
                pass
        self._label = label
        self._updated = False

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
            raise RuntimeError("Relay not commited")

        fdb = db.connect(self.database)

        while True:
            row = fdb.get(self.id_)

            status = row.get("status", "done")

            if status == "done" or not wait:
                break
            if status == "failed":
                raise RuntimeError(f"Run {self.id_} failed")

            time.sleep(cycle_time)

        return row

    row = property(
        get_row,
        doc=("Atoms row attached with the relay, awaits for the run to finish"),
    )

    def _to_dict(self):
        """
        Converts relay to dict. relay parents are saved as id.
        """
        dict_ = {}
        dict_["id"] = self.id_
        dict_["updated"] = self._updated
        dict_["database"] = self.database
        dict_["runnerdata"] = self.runnerdata.data
        dict_["runnername"] = self.runnername
        dict_["label"] = self.label
        dict_["parents"] = []
        for parent in self.parents:
            if isinstance(parent, Atoms):
                pass
                # dict_['parents'].append(parent.todict())
            elif isinstance(parent, int):
                dict_["parents"].append(parent)
            elif isinstance(parent, Relay):
                dict_["parents"].append(parent.id_)

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
        for parent in dict_["parents"]:
            if isinstance(parent, (int, str)):
                if parent in parent_dict:
                    parents.append(parent_dict[parent])
                else:
                    parents.append(parent)
            elif isinstance(parent, dict):
                parents.append(Atoms.fromdict(parent))
            else:
                raise RuntimeError(f"Unidentified parent: {parent}")

        relay = cls(
            dict_["label"],
            parents,
            RunnerData.from_data_dict(dict_["runnerdata"]),
            dict_["runnername"],
        )
        relay.id_ = dict_["id"]
        relay._updated = dict_["updated"]
        relay._database = dict_["database"]

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
        dict_["parent_dict"] = spider

        with open(filename, "w") as fio:
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

        return cls._parse_dict(data)

    @classmethod
    def _parse_dict(cls, data):
        """
        Parses raw dict data saved in json or database
        """
        # preparing parent_dict
        parent_dict_ = data.pop("parent_dict", {})
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

    @classmethod
    def _from_database(cls, index, fdb, parent_dict=None):
        """
        Helper function for from_database. Returns data per row
        """
        if parent_dict is None:
            parent_dict = {}
        row = fdb.get(index)

        data = {}
        data["id"] = index
        data["updated"] = True
        data["runnerdata"] = row.data.get("runner", {})
        data["runnername"] = row.get("runner", None)
        data["label"] = row.get("label", "")
        data["parents"] = row.data.get("runner", {}).get("parents", [])
        for parent in data["parents"]:
            _, parent_dict = cls._from_database(parent, fdb, parent_dict)
            parent_dict[parent] = _

        return data, parent_dict

    @classmethod
    def from_database(cls, index, database):
        """
        Get relay from database

        Args:
            index (int): id of row in database
            database (str): ASE database

        Returns:
            Relay object: relay associated with id
        """
        with db.connect(database) as fdb:
            data, parent_dict = cls._from_database(index, fdb)

        data["database"] = database
        for value in parent_dict.values():
            value["database"] = database
        data["parent_dict"] = parent_dict

        return cls._parse_dict(data)

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
                        raise RuntimeError("Cyclic connection detected. Abandon ship!")
                    dict_[parent.id_] = parent

                    dict_ = parent._spider(dict_, parent_call)

        parent_call.discard(self.id_)

        return dict_

    def get_relay_graph(self, filename, add_tasks=False):
        """
        Save the relay as a directional acyclic graph, with status

        Args:
            filename (str): png filename to save the graph
            add_tasks (bool): adds task information to the graph
        """
        get_graphical_status(
            filename, [self.id_], self, add_tasks=add_tasks, _get_info=_get_info
        )

    def replace_runnerdata(self, runnerdata):
        """
        Replace `RunnerData` for the relay and all relays with same runner
        name as the given `RunnerData`

        Args:
            runnerdata (:class:`~runner.RunnerData`): RunnerData to supplant.
        """
        runnername = runnerdata.name

        # replace self
        if self.runnerdata.name == runnername:
            self.runnerdata = runnerdata

        # replace parents
        parent_relays = self._spider()

        for key, parent_relay in parent_relays.items():
            if parent_relay.runnerdata.name == runnername:
                parent_relay.runnerdata = runnerdata


def _get_info(input_id, relay):
    """returns parent list, used in get_graphical_status"""
    if input_id != relay.id_:
        if isinstance(input_id, Atoms):
            return (input_id.get_chemical_formula(), None, [], "No status", [])
        if input_id in relay._spider():
            relay = relay._spider().get(input_id, None)
        else:
            parents = []
            tasks = []
            name = None
            if relay.database is None:
                status = "No status"
                formula = input_id
            else:
                with db.connect(relay.database) as fdb:
                    row = fdb.get(input_id)
                formula = row.formula
                status = row.get("status", "No status")
                if "runner" in row.data:
                    # no parents returned, since out of relay
                    # parents = row.data['runner'].get('parents', [])
                    tasks = row.data["runner"]["tasks"]
                    name = row.data["runner"]["name"]
            return formula, name, parents, status, tasks

    parents = [
        parent.id_ if isinstance(parent, Relay) else parent for parent in relay.parents
    ]
    tasks = relay.runnerdata.tasks
    name = relay.runnerdata.name
    formula = relay.__str__()
    if relay.database is None:
        status = "No status"
    else:
        with db.connect(relay.database) as fdb:
            row = fdb.get(relay.id_)
            status = row.get("status", "No status")
            if status == "done":
                formula = row.formula
    return formula, name, parents, status, tasks
