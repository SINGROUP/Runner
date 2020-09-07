from ase import db
import time
import os
from datetime import datetime
from abc import abstractmethod
import shutil
import pickle
import json
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')

file_handler = logging.FileHandler('sample.log')
file_handler.setLevel(logging.ERROR)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


class cd:
    """Context manager for changing the current working directory"""
    def __init__(self, new_path, mkdir=True):
        self.new_path = os.path.expanduser(new_path)
        if not os.path.exists(new_path) and mkdir:
            os.mkdir(new_path)

    def __enter__(self):
        self.saved_path = os.getcwd()
        os.chdir(self.new_path)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.saved_path)


class BaseRunner():

    def __init__(self,
                 name,
                 database="database.db",
                 max_jobs=50,
                 cycle_time=30,
                 keep_run=False,
                 run_folder='./',
                 multi_fail=0):
        """
        Runner runs tasks
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
                the folder that needs to be populated
            multi_fail: int
                The number of re-runs on failure
        """
        logger.debug('Initialising')
        self.name = name
        self.db = database
        self.max_jobs = max_jobs
        self.cycle_time = cycle_time
        self.keep_run = keep_run
        self.run_folder = os.path.abspath(run_folder)
        self.multi_fail = multi_fail

    def get_job_id(self, id_):
        """
        Returns job_id of id_
        """
        try:
            with cd(self.run_folder, mkdir=False):
                with cd(str(id_), mkdir=False):
                    with open('job.id') as f:
                        job_id = f.readline().strip()
            return job_id
        except FileNotFoundError:
            return None

    @abstractmethod
    def _submit(self,
                tasks,
                scheduler_options):
        """
        Abstract method to define submit
        Parameters
            tasks: list
                list of tasks
            scheduler_options: dictionary
                dictionary of headers to be added
        Returns
            job_id: str
                Job id of the successful run, None if failed
            log_msg: str
                log message of the run
        """
        raise NotImplementedError()

    @abstractmethod
    def _cancel(self, job_id):
        """
        cancel job id, if job id doesn't exist
        then do nothing and raise no error
        Parameters
            job_id: str or None
                job id to cancel
        Returns
            None
        """
        raise NotImplementedError()

    @abstractmethod
    def _status(self, job_id):
        """
        return status of job_id
        Parameters
            job_id: str
                job id of the run
        Returns
            status: str
                status of the job id
            log_msg: str
                log message of the last change
        """
        raise NotImplementedError()

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
                    with cd(self.run_folder, mkdir=False):
                        with cd(str(id_), mkdir=False):
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
                with cd(self.run_folder, mkdir=False):
                    with cd(str(id_), mkdir=False):
                        try:
                            # !TODO: remove reliance on pickle
                            with open('atoms.pkl', 'rb') as f:
                                atoms = pickle.load(f)
                        except:
                            status = 'failed:{}'.format(self.name)
                            log_msg += ('{}\n Unpickling failed\n'
                                        ''.format(datetime.now()))
                # make sure atoms is not list
                if isinstance(atoms, list):
                    atoms = atoms[0]
            # run post-tasks
            if status.startswith('done'):
                logger.debug('status: done')
                with db.connect(self.db) as fdb:
                    # getting data
                    logger.debug('getting data')
                    data = fdb.get(id_).data

                    # updating status and log
                    _ = data['scheduler'].get('log', '') + log_msg
                    data['scheduler']['log'] = _
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
                if not self.keep_run:
                    with cd(self.run_folder):
                        if str(id_) in os.listdir():
                            shutil.rmtree(str(id_))
            else:
                logger.debug('status:failed')
                with db.connect(self.db) as fdb:
                    # getting data
                    logger.debug('getting data')
                    data = fdb.get(id_).data

                    if status.startswith('failed'):
                        if 'fail_count' not in data['scheduler']:
                            data['scheduler']['fail_count'] = 1
                        else:
                            data['scheduler']['fail_count'] += 1

                    # updating status and log
                    _ = data['scheduler'].get('log', '') + log_msg
                    data['scheduler']['log'] = _
                    logger.debug('updating')
                    fdb.update(id_, status=status, data=data)

            # print status
            logger.info('Id {} finished with status: {}'.format(id_,
                                                                status))

    def get_status(self):
        '''
        Returns status of id_
        :return: status message
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
        with db.connect(self.db) as fdb:
            len_running = fdb.count(status='running:{}'.format(self.name))
            # submiting pending jobs
            sent_jobs = 0
            for row in fdb.select(status='submit:{}'.format(self.name)):
                id_ = row.id
                logger.debug('submit {}'.format(id_))
                # default status, no submission if changes
                status = 'submit:{}'.format(self.name)
                log_msg = ''
                # break if running jobs exceed
                if sent_jobs >= self.max_jobs - len_running:
                    logger.debug('max jobs; break')
                    break
                # get relevant data form atoms
                logger.debug('get scheduler data')
                (scheduler_options, parents, tasks, files,
                 log) = get_scheduler_data(row.data)

                # if error in scheduler data
                if tasks is None:
                    logger.info('scheduler data corrupt')
                    # job failed if no scheduler info
                    _ = scheduler_options or {}
                    _.update({'log': '{}\n{}\n'
                                     ''.format(datetime.now(), log),
                              'fail_count': self.multi_fail + 1})
                    row.data['scheduler'] = _
                    fdb.update(id_,
                               status='failed:{}'.format(self.name),
                               data=row.data)
                    continue

                # get self and parents atoms object with everything
                logger.debug('getting atoms and parents')
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
                    if fdb.get(i).status.startswith('done'):
                        parents_done = False
                        break
                    atoms.append(fdb.get_atoms(i))

                if not parents_done:
                    logger.debug('parents pending')
                    continue

                with cd(self.run_folder):
                    with cd(str(id_)):
                        # submitting task
                        logger.debug('submitting {}'.format(id_))

                        # write files
                        for i, string in files.items():
                            with open(i, 'w') as f:
                                f.write(string)

                        # write atoms
                        with open('atoms.pkl', 'wb') as f:
                            pickle.dump(atoms, f)

                        # preparing run script
                        run_scripts = []
                        py_run = 0
                        for task in tasks:
                            # if task is a shell
                            if task[0] == 'shell':
                                shell_run = task[1]
                                if isinstance(shell_run, list):
                                    shell_run = ' '.join(shell_run)
                                run_scripts.append(shell_run)
                            elif task[0].endswith('python'):
                                # python run
                                shell_run = task[0]
                                shell_run += ' run{}.py'.format(py_run)
                                run_scripts.append(shell_run)

                                params = task[1][1]
                                if isinstance(params, (tuple, list)):
                                    astr = '*'
                                elif isinstance(params, dict):
                                    astr = '**'
                                # write params
                                try:
                                    with open('params{}.json'.format(py_run),
                                              'w') as f:
                                        json.dump(params, f)
                                except TypeError as e:
                                    status = 'failed'
                                    log_msg = ('{}\n Error writing params: '
                                               '{}\n'.format(datetime.now(),
                                                             e.args[0]))
                                # making python executable
                                run_py = """import json
import pickle
from ase.atoms import Atoms

{func}

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
                                """.format(func=task[1][0],
                                           ind=py_run,
                                           astr=astr)

                                with open('run{}.py'.format(py_run), 'w') as f:
                                    f.write(run_py)

                                # increment py_run
                                py_run += 1

                        if status.startswith('submit'):
                            job_id, log_msg = self._submit(run_scripts,
                                                           scheduler_options)
                            if job_id:
                                logger.debug('submitting success {}'
                                             ''.format(job_id))
                                # update status and save job_id
                                status = 'running:{}'.format(self.name)
                                with open('job.id', 'w') as f:
                                    f.write('{}'.format(job_id))
                                sent_jobs += 1
                            else:
                                logger.debug('submitting failed {}'
                                             ''.format(job_id))
                                status = 'failed:{}'.format(self.name)

                        # updating database
                        data = row.data
                        _ = data['scheduler'].get('log', '') + log_msg
                        data['scheduler']['log'] = _
                        logger.debug('updating database')
                        fdb.update(id_, status=status, data=data)
                logger.info('ID {} submission: '
                            '{}'.format(id_,
                                        (status if status == 'failed' else
                                         'successful')))

    def _cancel_run(self):
        """
        Cancels run in cancel
        """
        with db.connect(self.db) as fdb:
            for row in fdb.select(status='cancel:{}'.format(self.name)):
                id_ = row.id
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
                data = fdb.get(id_).data
                _ = data['scheduler'].get('log', '') + log_msg
                data['scheduler']['log'] = _
                logger.debug('update {}'.format(id_))
                fdb.update(id_, status=status, data=data)

    def spool(self):
        '''
        Does the spooling of jobs
        '''
        while True:
            logger.info('Searching failed jobs')
            with db.connect(self.db) as fdb:
                for row in fdb.select(status='failed:{}'.format(self.name)):
                    id_ = row.id
                    update = False
                    if 'scheduler' not in row.data:
                        row.data['scheduler'] = {}
                    if 'fail_count' not in row.data['scheduler']:
                        row.data['scheduler']['fail_count'] = (self.multi_fail
                                                               + 1)
                        update = True
                    if (row.data['scheduler']['fail_count']
                            <= self.multi_fail):
                        # submit in next cycle
                        # multiple updates for same id_ in "with"
                        # statement is problematic
                        logger.debug('re-submitted: {}'.format(id_))
                        update = True
                    if update:
                        fdb.update(id_, status='submit:{}'.format(self.name),
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

            # sleep before checking again
            logger.info('Sleeping for {}s'.format(self.cycle_time))
            time.sleep(self.cycle_time)

            # check if stop file exists
            if 'STOP' in os.listdir(self.run_folder):
                logger.info('STOP file found; stopping')
                break


def get_scheduler_data(data):
    """
    helper function to get complete scheduler data
    Example:
        {'scheduler_options': {'nodes': 1,
                               'tot_cores': 16,
                               'time': '0:5:0:0',
                               'mem': 2000},
         'parents': [],
         'tasks': [['python', ['<script>', <params>]], # python run
                   ['mpirun -n 4  python', ['<script>', <params>]], # parallel
                   ['shell', '<command>']] # any shell command
         'log': ''}
    Parameters
        data: dict
            scheduler data in atoms data
    Returns
        scheduler_options: dict
            containing all options to run a job
        parents: list
            list of parents attached to the present job
        tasks: list
            list of tasks to perform
        files: dict
            dictionary of filenames as key and strings as value
    """
    success = True
    log = '{}\n'.format(datetime.now())
    data = data.get('scheduler', None)

    if data is None:
        success = False
        log += 'No scheduler data\n'
    else:
        scheduler_options = data.get('scheduler_options', None)
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
            for i in range(len(tasks)):
                task = tasks[i]
                if str(task[0]) == 'shell' or str(task[0]).endswith('python'):
                    success = False
                    log += ("Scheduler: task should either be 'shell' or "
                            "'<optional prefix> python'\n")

                    if task[0].endswith('python'):
                        if isinstance(task[1], str):
                            # add empty params
                            task[1] = [task[1], []]
                        elif not isinstance(task[1], (list, tuple)):
                            success = False
                            log += ('Scheduler: python task should be a list '
                                    'with file string and parameters\n')
                        elif len(task[1]) == 0:
                            success = False
                            log += ('Scheduler: python task should be a list '
                                    'with file string and parameters\n')
                        elif len(task[1]) == 1:
                            # add empty params
                            task[1] = [task[1], []]
                        elif len(task[1]) > 2:
                            success = False
                            log += ('Scheduler: python task should be a list '
                                    'with file string and parameters\n')
                        elif isinstance(task[1][1], (list, tuple, dict)):
                            success = False
                            log += ('Scheduler: python parameters should'
                                    ' be either list of dict')

    if success:
        return (scheduler_options, parents, tasks, files, log)
    else:
        return (data, None, None, None, log)
