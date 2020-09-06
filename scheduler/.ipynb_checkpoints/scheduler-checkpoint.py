import numpy as np
from ase import db
import time
import os
from datetime import datetime
import json


class cd:
    """Context manager for changing the current working directory"""
    def __init__(self, new_path):
        self.new_path = os.path.expanduser(new_path)
        if not os.path.exists(new_path):
            os.mkdir(new_path)

    def __enter__(self):
        self.saved_path = os.getcwd()
        os.chdir(self.new_path)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.saved_path)


class Scheduler():

    def __init__(self,
                 database,
                 max_jobs=50,
                 cycle_time=30,
                 keep_run=False,
                 run_folder='./',
                 functions_folder='functions',
                 job_ids='job_ids.json'):
        '''
        Scheduler submits runs
        Parameters
            database: ASE database
                the database to connect
            max_jobs: int
                maximum number of jobs running at an instance
            cycle_time: int
                time in seconds
            keep_run: bool
                keep the folder in which the run was performed
            run_folder: str
                the forder that needs to be populated
            functions_folder: str
                the folder with all the helper functions inside run_folder
            job_ids: str or dict
                path to json containing job_ids for rows in database
                inside run_folder or dict containing the same
        '''
        self.db = database
        self.max_jobs = max_jobs
        self.cycle_time = cycle_time
        self.keep_run = keep_run
        self.run_folder = os.path.abspath(run_folder)
        self.functions_folder = os.path.join(self.run_folder,
                                             functions_folder)
        self.job_ids = job_ids

    @property
    def job_ids(self):
        """returns job_ids in job_id_file"""
        with open(self._job_id_file) as f:
            job_ids = json.load(f)
        return job_ids

    @job_ids.setter
    def job_ids(self, value):
        """sets job ids
        Parameters
            value: str or dict
                str is the location of job_ids json file or dict is
                a dictionary of id_, job_id pairs
        """
        if isinstance(value, str):
            # if string, then add the file and check its contents
            self._job_id_file = os.path.abspath(value)
            with open(self._job_id_file) as f:
                # will raise errors if json file is improper
                _ = json.load(f)
        else:
            if hasattr(self, '_job_id_file'):
                # json file exists
                # load those job ids
                with open(self._job_id_file) as f:
                    job_ids = json.load(f)
            else:
                # add a json file
                self._job_id_file = os.path.abspath('./job_ids.json')
                # make empty job ids dict
                job_ids = {}
            job_ids.update(value)
            # update json file
            with open(self._job_id_file, 'w') as f:
                json.dump(job_ids, f)

    def _update_status_submitted(self, submitted):
        '''
        changes submitted to failed or done if finished
        :param submitted: list of ids
        :return: list of updated ids that are still submitted
        '''
        update_ids_status = {}
        with db.connect(self.db) as fdb:
            for i, id_ in submitted:
                job_id = self.job_ids.get(id_, None)
                status, log_msg = self._status(job_id)
                if status != 'submitted':
                    # if not still submitted, update status and add log message
                    update_ids_status[id_] = [status, log_msg]
                elif fdb.get(id_).status == 'cancel':
                    # cancel the job and update database
                    self._cancel(job_id)
                    update_ids_status[id_] = ['failed',
                                              '{}\nCancelled by user'
                                              ''.format(datetime.now())]

        #! TODO: get failed log
        for id_, values in update_ids_status.items():
            status, log_msg = values
            submitted.pop(submitted.index(id_))
            self.db.update(id_, status=status)

            # print status
            print('Id {} finished with status: {}'.format(id_,
                                                          status))

            # delete run if done and keep_run is False
            if status == 'done' and not self.keep_run:
                with cd(self.run_folder):
                    if str(id_) in os.listdir():
                        os.rmdir(id_)

        return submitted

    def get_status(self, id_=None, verbose=False):
        '''
        Returns status of id_
        :param id_: list of ints, the ids to be used
        :param verbose: bool, show status of finished jobs
        :return: status message
        '''
        if id_ is None:
            id_ = np.arange(len())


    def _submit_pending(self, pending):
        '''
        submits runs
        :param pending: list, of pending ids
        '''
        # submiting pending jobs
        for id_ in pending:
            # get atoms object will everything
            atoms = self.db.get_atoms(id_)

            # getting tasks
            functions = ''
            with cd(self.run_folder):
                with cd(id_):
                    # writing input files
                    job_id, log_msg = self._submit(atoms,
                                                   self.helper_functions)
                    if job_id:
                        self.db.update(id_, status='submitted')
                        print('{} submitted at {}'.format(id_, job_id))
                    else:
                        self.db.update(id_, status='failed')
                        print('Error submitting id {}: {}'.format(id_,
                                                                  log_msg))


    def spooling(self):
        '''
        Does the spooling of jobs
        '''
        while True:
            # list of submitted jobs that are supposed to be running
            submitted = []
            # list of pending jobs
            pending = []
            with db.connect(self.db) as fdb:
                for id_ in range(1, len(fdb) + 1):
                    # get a row
                    row = fdb.get(id_)
                    if not hasattr(row, 'status'):
                        # if row has no status, then update db
                        # and add status of pending
                        fdb.update(id_, status='pending')
                        row.status = 'pending'
                    if row.status == 'pending':
                        pending.append(id_)
                    elif row.status == 'submitted':
                        submitted.append(id_)

            # update if submitted have finished
            submitted = self._update_status_submitted(submitted)

            # send pending for run
            self._submit_pending(pending[:self.max_jobs - len(submitted)])

            # sleep before checking again
            time.sleep(self.cycle_time)
