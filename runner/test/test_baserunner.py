import time
from copy import copy
import os

import pytest
import ase.db as db
from ase.atoms import Atoms

from runner import TerminalRunner


energy = """\
from ase.atoms import Atoms

def main(atoms, conf=0):
    assert conf == 0, 'bad params'
    assert isinstance(atoms[0], Atoms), 'bad atoms'
    return atoms
"""
runner = {'scheduler_options': {},
          'tasks': [['shell', 'sleep 1'],
                    ['python', 'energy.py'],
                    ['python', 'energy.py', {'conf': 0}],
                    ['python', 'energy.py', {'conf': 0}, 'python3'],
                   ],
          'files': {'energy.py': energy}
         }

def test_successful_run():
    """test run and parent run"""
    with db.connect('database.db') as fdb:
        data = {'runner': copy(runner)}
        id_ = fdb.write(Atoms(), data=data, status='submit:terminal:test')
        data['runner']['parents'] = [id_]
        id_1 = fdb.write(Atoms(), data=data, status='submit:terminal:test')
        # waiting on next pass
        data = {'runner': copy(runner)}
        data['runner']['tasks'][0][1] = 'sleep 7'
        id_2 = fdb.write(Atoms(), data=data, status='submit:terminal:test')
        # test max jobs and keep run
        data['runner']['tasks'][0][1] = 'sleep 1'
        data['runner']['keep_run'] = True
        id_3 = fdb.write(Atoms(), data=data, status='submit:terminal:test')

    run = TerminalRunner('test', max_jobs=2)
    run.spool(_endless=False)
    fdb = db.connect('database.db')
    assert fdb.get(id_).status == 'running:terminal:test'
    assert fdb.get(id_1).status == 'submit:terminal:test'
    assert fdb.get(id_2).status == 'running:terminal:test'
    assert fdb.get(id_3).status == 'submit:terminal:test'
    time.sleep(5)
    run.spool(_endless=False)
    assert fdb.get(id_).status == 'done:terminal:test'
    assert fdb.get(id_1).status == 'running:terminal:test'
    assert fdb.get(id_2).status == 'running:terminal:test'
    assert fdb.get(id_3).status == 'submit:terminal:test'
    time.sleep(5)
    run.spool(_endless=False)
    assert fdb.get(id_).status == 'done:terminal:test'
    assert fdb.get(id_1).status == 'done:terminal:test'
    assert fdb.get(id_2).status == 'done:terminal:test'
    assert fdb.get(id_3).status == 'running:terminal:test'
    time.sleep(5)
    run.spool(_endless=False)
    assert fdb.get(id_3).status == 'done:terminal:test'

    assert not str(id_) in os.listdir(), 'no cleanup after done' 
    assert not str(id_1) in os.listdir(), 'no cleanup after done' 
    assert not str(id_2) in os.listdir(), 'no cleanup after done' 
    assert str(id_3) in os.listdir(), 'keep_run failed' 

def test_failed_run():
    """test failed run"""
    id_ = [None for _ in range(4)]
    with db.connect('database.db') as fdb:
        # job id lost
        data = {'runner': copy(runner)}
        data['runner']['tasks'].append(['shell', 'rm job.id'])
        id_[0] = fdb.write(Atoms(),
                           data=data,
                           status='submit:terminal:test')
        # unpickling fail
        data['runner']['tasks'][4][1] = 'cp run.sh atoms.pkl'
        id_[1] = fdb.write(Atoms(),
                           data=data,
                           status='submit:terminal:test')
        # bad runner data
        data = {'runner': copy(runner)}
        data['runner']['tasks'][1][1] = 'energy1.py'
        id_[2] = fdb.write(Atoms(),
                           data=data,
                           status='submit:terminal:test')
        data['runner']['tasks'] = []
        id_[3] = fdb.write(Atoms(),
                           data=data,
                           status='submit:terminal:test')


    run = TerminalRunner('test')
    run.spool(_endless=False)
    fdb = db.connect('database.db')
    for i in id_:
        if i in [id_[x] for x in [2, 3]]:
            # bad input fails instantly
            assert fdb.get(i).status == 'failed:terminal:test', i
        else:
            assert fdb.get(i).status == 'running:terminal:test', i
    time.sleep(5)
    run.spool(_endless=False)
    for i in id_:
        assert fdb.get(i).status == 'failed:terminal:test', i
        if i == id_[0]:
            assert str(i) in os.listdir(), 'failed run folder cleaned' 
            assert fdb.get(i).data['runner']['fail_count'] == 1,\
                    'fail count not updated'
