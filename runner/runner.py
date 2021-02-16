"""
Base runner for different runnners
"""

import shutil
import pickle
import json
import logging
import time
import os
from datetime import datetime
from abc import ABC, abstractmethod
from ase import db
from ase import Atoms
from runner.utils import Cd, RUN_PY
from runner.utils.runnerdata import RunnerData

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(stream_handler)


default_files = ['run.sh', 'batch.slrm', 'atoms.pkl']


class BaseRunner(ABC):
    """
    Runner runs tasks

    Args:
        database (str): ASE database to connect
        interpreter (str): the interpreter for the shell
        scheduler_options (dict): scheduler_options local to the system
        tasks (list): pre-tasks local to the system
        files (dict): pre-tasks files local to the system
        max_jobs (int): maximum number of jobs running at an instance
        cycle_time (int): time in seconds
        keep_run (bool): keep the folder in which the run was performed
        run_folder (str): the folder that needs to be populated
        multi_fail (int): The number of re-runs on failure
        logfile (str): log file for logging
    """

    def __init__(self,
                 name,
                 database="database.db",
                 interpreter="#!/bin/bash",
                 scheduler_options=None,
                 tasks=None,
                 files=None,
                 max_jobs=50,
                 cycle_time=30,
                 keep_run=False,
                 run_folder='./',
                 multi_fail=0,
                 logfile=None):

        # logging
        if logfile:
            file_handler = logging.FileHandler(logfile)
            file_handler.setLevel(logging.ERROR)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        logger.debug('Initialising')
        self.name = name
        self.db = database
        self.max_jobs = max_jobs
        self.cycle_time = cycle_time
        self.keep_run = keep_run
        self.run_folder = os.path.abspath(run_folder)
        self.multi_fail = multi_fail
        self.interpreter = interpreter

        runner = {}
        if scheduler_options is not None:
            runner['scheduler_options'] = scheduler_options
        if tasks is not None:
            runner['tasks'] = tasks
        if files is not None:
            runner['files'] = files
        runner_data = RunnerData(runner)
        (scheduler_options, name, parents, tasks,
         files) = runner_data.get_runner_data(_skip_empty_task_test=True)

        self.tasks = tasks
        self.scheduler_options = scheduler_options
        self.files = files

    def to_database(self, update=False):
        """attaches runner to database

        Args:
            update (bool): optional, update runner info if already exists"""
        dict_ = {}
        dict_['max_jobs'] = self.max_jobs
        dict_['cycle_time'] = self.cycle_time
        dict_['keep_run'] = self.keep_run
        dict_['run_folder'] = self.run_folder
        dict_['multi_fail'] = self.multi_fail
        dict_['interpreter'] = self.interpreter
        dict_['tasks'] = self.tasks
        dict_['scheduler_options'] = self.scheduler_options
        dict_['files'] = self.files
        dict_['running'] = False

        with db.connect(self.db) as fdb:
            meta = fdb.metadata

        runners = meta.get('runners', {})

        if self.name in runners:
            if not update:
                raise RuntimeError('Runner exists, pass argument update as'
                                   ' true to update')
            elif runners[self.name].get('running', False):
                raise RuntimeError('Runner already running')

        if 'runners' not in meta:
            meta['runners'] = {}

        meta['runners'].update({self.name: dict_})
        with db.connect(self.db) as fdb:
            fdb.metadata = meta

    @classmethod
    def from_database(cls, name, database):
        """Get runner from database

        Args:
            name (str): name of runner
            database (str): database

        Returns:
            :class:`~runner.runner.BaseRunner`: returns relevant runner class
        """

        with db.connect(database) as fdb:
            meta = fdb.metadata

        try:
            dict_ = meta['runners'][name]
        except KeyError:
            raise KeyError(f'{name} not in runners, try runner list')
        dict_.pop('running', False)

        return cls(name=name,
                   database=database,
                   **dict_)

    def _set_running(self):
        """notify database that runner is running"""
        with db.connect(self.db) as fdb:
            meta = fdb.metadata
            meta['runners'][self.name]['running'] = True
            fdb.metadata = meta

    def _unset_running(self):
        """notify database that runner is not running"""
        with db.connect(self.db) as fdb:
            meta = fdb.metadata
            meta['runners'][self.name]['running'] = False
            fdb.metadata = meta

    def get_job_id(self, input_id):
        """
        Returns job id (slurm id, process id etc) depending on workflow
        manager of a running row.

        Args:
            input_id (int): Row id

        Returns:
            int or None: job_id of input_id if running, else None
        """
        try:
            with Cd(self.run_folder, mkdir=False):
                with Cd(str(input_id), mkdir=False):
                    with open('job.id') as file_o:
                        job_id = file_o.readline().strip()
            return job_id
        except FileNotFoundError:
            return None

    @abstractmethod
    def _submit(self,
                tasks,
                scheduler_options):
        """
        Abstract method to define submit

        Args:
            tasks (list): list of tasks
            scheduler_options (dict): dictionary of headers to be added

        Returns:
            str: Job id of the successful run, None if failed
            str: log message of the run
        """
        pass

    @abstractmethod
    def _cancel(self, job_id):
        """
        cancel job id, if job id doesn't exist
        then do nothing and raise no error

        Args:
            job_id (str or None): job id to cancel

        Returns:
            None
        """
        pass

    @abstractmethod
    def _status(self, job_id):
        """
        return status of job_id

        Args:
            job_id (str): job id of the run

        Returns:
            str: status of the job id
            str: log message of the last change
        """
        pass

    def _update_status_running(self):
        """
        changes running to failed or done if finished
        """
        # get status of running jobs
        update_ids_status = {}
        with db.connect(self.db) as sdb:
            for row in sdb.select(status='running:{}'.format(self.name),
                                  columns=['id']):
                id_ = row.id
                logger.debug('getting job id {}'.format(id_))
                job_id = self.get_job_id(id_)
                if job_id:
                    logger.debug('job_id success; getting status')
                    # !TODO: update scheduler options with cpu usage
                    with Cd(self.run_folder, mkdir=False):
                        with Cd(str(id_), mkdir=False):
                            status, log_msg = self._status(job_id)
                else:
                    # oops
                    logger.debug('job_id fail; updating status')
                    status, atoms, log_msg = ['failed:{}'.format(self.name),
                                              None,
                                              '{}\nJob id lost\n'
                                              ''.format(datetime.now())]
                if not status.startswith('running'):
                    # if not still running, update status and add log message
                    update_ids_status[id_] = [status, log_msg]

        # update status for jobs that stopped running
        for id_, values in update_ids_status.items():
            status, log_msg = values
            logger.debug('ID {} finished'
                         ''.format(id_))

            if status.startswith('done'):
                with Cd(self.run_folder, mkdir=False):
                    with Cd(str(id_), mkdir=False):
                        try:
                            # !TODO: remove reliance on pickle
                            with open('atoms.pkl', 'rb') as file_o:
                                atoms = pickle.load(file_o)
                            # make sure atoms is not list
                            if isinstance(atoms, list):
                                atoms = atoms[0]
                            assert isinstance(atoms, Atoms)
                        except Exception:
                            status = 'failed:{}'.format(self.name)
                            log_msg += ('{}\n Unpickling failed\n'
                                        ''.format(datetime.now()))
            # run post-tasks
            if status.startswith('done'):
                logger.debug('status: done')
                with db.connect(self.db) as fdb:
                    # getting data
                    logger.debug('getting data')
                    data = fdb.get(id_).data

                    # updating status and log
                    _ = data['runner'].get('log', '') + log_msg
                    data['runner']['log'] = _
                    # remove old data
                    atoms.info.pop('data', None)
                    atoms.info.pop('unique_id', None)
                    key_value_pairs = atoms.info.pop('key_value_pairs', {})
                    # update data
                    key_value_pairs['status'] = status
                    data.update(atoms.info)
                    logger.debug('updating')
                    fdb.update(id_, atoms=atoms, data=data,
                               **key_value_pairs)
                # delete run if keep_run is False
                if not self.keep_run and not data['runner'].get('keep_run',
                                                                False):
                    with Cd(self.run_folder):
                        if str(id_) in os.listdir():
                            shutil.rmtree(str(id_))
            else:
                logger.debug('status:failed')
                with db.connect(self.db) as fdb:
                    # getting data
                    logger.debug('getting data')
                    data = fdb.get(id_).data

                    if status.startswith('failed'):
                        if 'fail_count' not in data['runner']:
                            data['runner']['fail_count'] = 1
                        else:
                            data['runner']['fail_count'] += 1

                    # updating status and log
                    _ = data['runner'].get('log', '') + log_msg
                    data['runner']['log'] = _
                    logger.debug('updating')
                    fdb.update(id_, status=status, data=data)

            # print status
            logger.info('Id {} finished with status: {}'.format(id_,
                                                                status))

    def get_status(self):
        '''
        Returns ids of each status

        Returns:
            dict: dictionary of status, ids list
        '''
        # connect database
        fdb = db.connect(self.db)

        # status dict with ids in different status
        status_dict = {'done:{}'.format(self.name): [],
                       'running:{}'.format(self.name): [],
                       'pending:{}'.format(self.name): [],
                       'failed:{}'.format(self.name): [],
                       'cancel:{}'.format(self.name): [],
                       'submit:{}'.format(self.name): []}

        for status in status_dict:
            for row in fdb.select(status=status, columns=['id']):
                status_dict[status].append(row.id)

        return status_dict

    def _submit_run(self):
        '''
        submits runs
        '''
        ids_ = []
        with db.connect(self.db) as fdb:
            len_running = fdb.count(status='running:{}'.format(self.name))
            for row in fdb.select(status='submit:{}'.format(self.name)):
                ids_.append(row.id)
        # submiting pending jobs
        sent_jobs = 0
        for id_ in ids_:
            logger.debug('submit {}'.format(id_))
            # default status, no submission if changes
            status = 'submit:{}'.format(self.name)
            log_msg = ''
            # break if running jobs exceed
            if sent_jobs >= self.max_jobs - len_running:
                logger.debug('max jobs; break')
                break
            with db.connect(self.db) as fdb:
                row = fdb.get(id_)
                # get relevant data form atoms
                logger.debug('get runner data')
                runnerdata = RunnerData(row.data.get('runner', None))
                try:
                    (scheduler_options, name, parents, tasks,
                     files) = runnerdata.get_runner_data()
                except RuntimeError as err:
                    logger.info('scheduler data corrupt/missing')
                    # job failed if corrupt/missing scheduler info
                    _ = row.data.get('runner', {})
                    _.update({'log': '{}\n{}\n'
                                     ''.format(datetime.now(), err),
                              'fail_count': self.multi_fail + 1})
                    row.data['runner'] = _
                    fdb.update(id_,
                               status='failed:{}'.format(self.name),
                               data=row.data)
                    continue

            # add local runner things
            scheduler_options.update(self.scheduler_options)
            files.update(self.files)
            tasks = self.tasks + tasks  # prior execution of local tasks

            # get self and parents atoms object with everything
            logger.debug('getting atoms and parents')
            with db.connect(self.db) as fdb:
                row = fdb.get(id_)
                atoms = []
                try:
                    atoms.append(row.toatoms(attach_calculator=True,
                                             add_additional_information=True))
                except AttributeError:
                    atoms.append(row.toatoms(attach_calculator=False,
                                             add_additional_information=True))

                # if any parent is not done, then don't submit
                parents_done = True
                for i in parents:
                    parent_row = fdb.get(i)
                    if not parent_row.status.startswith('done'):
                        parents_done = False
                        break
                    # !TODO: catch exception if user does not have permission
                    # to read parent
                    try:
                        _ = parent_row.toatoms(attach_calculator=True,
                                               add_additional_information=True)
                    except AttributeError:
                        _ = parent_row.toatoms(attach_calculator=False,
                                               add_additional_information=True)
                    parent = _
                    atoms.append(parent)

            if not parents_done:
                logger.debug('parents pending')
                continue

            with Cd(self.run_folder):
                with Cd(str(id_)):
                    # submitting task
                    logger.debug('submitting {}'.format(id_))

                    # preparing run script
                    (run_scripts,
                     status,
                     log_msg) = self._write_run_data(atoms,
                                                     tasks,
                                                     files,
                                                     status,
                                                     log_msg)
                    if status.startswith('submit'):
                        job_id, log_msg = self._submit(run_scripts,
                                                       scheduler_options)
                        if job_id:
                            logger.debug('submitting success {}'
                                         ''.format(job_id))
                            # update status and save job_id
                            status = 'running:{}'.format(self.name)
                            with open('job.id', 'w') as file_o:
                                file_o.write('{}'.format(job_id))
                            sent_jobs += 1
                        else:
                            logger.debug('submitting failed {}'
                                         ''.format(job_id))
                            status = 'failed:{}'.format(self.name)

            with db.connect(self.db) as fdb:
                row = fdb.get(id_)
                # updating database
                data = row.data
                _ = data['runner'].get('log', '') + log_msg
                data['runner']['log'] = _
                logger.debug('updating database')
                # adds status, name of calculation, and data
                fdb.update(id_, status=status, name=name, data=data)
            logger.info('ID {} submission: '
                        '{}'.format(id_,
                                    (status if status.startswith('failed')
                                     else 'successful')))

    def _write_run_data(self, atoms, tasks, files, status, log_msg):
        """
        writes run data in the folder for excecution
        """
        # write files
        for i, string in files.items():
            write_mode = ('wb' if isinstance(string, bytes) else 'w')
            with open(i, write_mode) as file_o:
                file_o.write(string)

        # write atoms
        with open('atoms.pkl', 'wb') as file_o:
            pickle.dump(atoms, file_o)

        # write run scripts
        run_scripts = []
        py_run = 0
        for task in tasks:
            if task[0] == 'shell':
                # shell run
                shell_run = task[1]
                if isinstance(shell_run, list):
                    shell_run = ' '.join(shell_run)
                run_scripts.append(shell_run)
            elif task[0] == 'python':
                # python run
                shell_run = 'python'
                if len(task) > 3:
                    shell_run = task[3]
                shell_run += ' run{}.py'.format(py_run)
                shell_run += ' > run{}.out'.format(py_run)
                run_scripts.append(shell_run)

                if len(task) > 2:
                    params = task[2]
                else:
                    params = {}
                # write params
                try:
                    with open('params{}.json'.format(py_run), 'w') as file_o:
                        json.dump(params, file_o)
                except TypeError as err:
                    status = 'failed:{}'.format(self.name)
                    log_msg = ('{}\n Error writing params: '
                               '{}\n'.format(datetime.now(),
                                             err.args[0]))
                    break
                # making python executable
                func_name = task[1]
                func_name = (func_name[:-3] if func_name.endswith('.py') else
                             func_name)
                with open('run{}.py'.format(py_run), 'w') as file_o:
                    file_o.write(RUN_PY.format(func=func_name,
                                               ind=py_run))
            py_run += 1

        return run_scripts, status, log_msg

    def _cancel_run(self):
        """
        Cancels run in cancel
        """
        ids_ = []
        with db.connect(self.db) as fdb:
            for row in fdb.select(status='cancel:{}'.format(self.name)):
                ids_.append(row.id)

        for id_ in ids_:
            logger.debug('cancel {}'.format(id_))
            job_id = self.get_job_id(id_)
            if job_id:
                logger.debug('found {}'.format(id_))
                # cancel the job and update database
                self._cancel(job_id)
                status, log_msg = ['failed:{}'.format(self.name),
                                   '{}\nCancelled by user\n'
                                   ''.format(datetime.now())]
            else:
                logger.debug('lost {}'.format(id_))
                # no job_id but still cancel, eg when pending
                status, log_msg = ['failed:{}'.format(self.name),
                                   '{}\nCancelled by user, '
                                   'no job was running\n'
                                   ''.format(datetime.now())]
            # updating status and log
            with db.connect(self.db) as fdb:
                data = fdb.get(id_).data
                _ = data['runner'].get('log', '') + log_msg
                data['runner']['log'] = _
                logger.debug('update {}'.format(id_))
                fdb.update(id_, status=status, data=data)

    def spool(self, _endless=True):
        '''
        Does the spooling of jobs
        '''
        # since the user is now spooling, the runner should update the
        # metadata of the database, this will raise error if the runner
        # is already running
        self.to_database(update=True)
        # now set the runner as running
        self._set_running()
        try:
            while True:
                logger.info('Searching failed jobs')
                with db.connect(self.db) as fdb:
                    for row in fdb.select(status=f'failed:{self.name}'):
                        id_ = row.id
                        update = False
                        if 'runner' not in row.data:
                            row.data['runner'] = {}
                        if 'fail_count' not in row.data['runner']:
                            row.data['runner']['fail_count'] = (self.multi_fail
                                                                + 1)
                            update = True
                        if (row.data['runner']['fail_count']
                                <= self.multi_fail):
                            # submit in next cycle
                            # multiple updates for same id_ in "with"
                            # statement is problematic
                            logger.debug('re-submitted: {}'.format(id_))
                            update = True
                        if update:
                            fdb.update(id_, status=f'submit:{self.name}',
                                       data=row.data)

                # cancel jobs
                logger.info('Cancelling jobs, if jobs to cancel')
                self._cancel_run()

                # update if running have finished
                logger.info('Updating running status')
                self._update_status_running()

                # send submit for run
                logger.info('Submitting')
                self._submit_run()

                if _endless:
                    # sleep before checking again
                    logger.info('Sleeping for {}s'.format(self.cycle_time))
                    time.sleep(self.cycle_time)
                else:
                    # used for testing
                    break
        except KeyboardInterrupt:
            pass
        finally:
            self._unset_running()

