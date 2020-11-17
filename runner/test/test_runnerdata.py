import pytest
from runner.utils.runnerdata import RunnerData


def test_runnerdata():
    energy_calculation = 'import numpy as np'
    success = {'name': 'energy calculation',
               'tasks': [['shell', "export OMP_NUM_THREADS=1"],
                         ['python', "energy.py"],
                         ['python', "energy.py", {'conf': 0}],
                         ['python', "energy.py", {'conf': 0}, 'python3']
                        ],
               'files': {'energy.py': energy_calculation},
               'parents': [1, 2],
               'scheduler_options': {'-N': 5},
               'keep_run': True
              }

    run = RunnerData(success)
    (scheduler_options, name, parents, tasks,
     files) = run.get_runner_data()
    assert name == 'energy calculation'
    assert len(parents) == 2
    assert parents[0] == 1 and parents[1] == 2
    assert isinstance(scheduler_options, dict)
    assert isinstance(files, dict)
    assert 'energy.py' in files
    assert len(tasks) == 4

    # empty tasks with skip test
    success = {'name': 'energy calculation',
               'tasks': [],
               'files': {'energy.py': energy_calculation},
               'scheduler_options': {'-N': 5},
               'keep_run': True
              }

    run = RunnerData(success)
    (scheduler_options, name, parents, tasks,
     files) = run.get_runner_data(_skip_empty_task_test=True)
    assert name == 'energy calculation'
    assert isinstance(scheduler_options, dict)
    assert isinstance(files, dict)
    assert 'energy.py' in files
    assert len(tasks) == 0

    fail = [None for _ in range(16)]
    # empty tasks
    fail[1] = {'name': 'energy calculation',
               'tasks': [],
               'files': {'energy.py': energy_calculation},
               'scheduler_options': {'-N': 5},
               'keep_run': True
              }
    # files not dictionary
    fail[2] = {'name': 'energy calculation',
               'tasks': [['shell', "export OMP_NUM_THREADS=1"],
                         ['python', "energy.py"]
                        ],
               'files': [],
               'scheduler_options': {'-N': 5},
               'keep_run': True
              }
    # task file not in files
    fail[3] = {'name': 'energy calculation',
               'tasks': [['shell', "export OMP_NUM_THREADS=1"],
                         ['python', "energy1.py"]
                        ],
               'files': {'energy.py': energy_calculation},
               'scheduler_options': {'-N': 5},
               'keep_run': True
              }
    # params is str, not tuple, list or dict
    fail[4] = {'name': 'energy calculation',
               'tasks': [['shell', "export OMP_NUM_THREADS=1"],
                         ['python', "energy.py", 'params']
                        ],
               'files': {'energy.py': energy_calculation},
               'scheduler_options': {'-N': 5},
               'keep_run': True
              }
    # parents are not a list
    fail[5] = {'name': 'energy calculation',
               'tasks': [['shell', "export OMP_NUM_THREADS=1"],
                         ['shell', "module load CP2K"],
                         ['python', "energy.py"]
                        ],
               'parents': '1, 2',
               'files': {'energy.py': energy_calculation},
               'scheduler_options': {'-N': 5},
               'keep_run': True
              }
    # parents not int
    fail[6] = {'name': 'energy calculation',
               'tasks': [['shell', "export OMP_NUM_THREADS=1"],
                         ['shell', "module load CP2K"],
                         ['python', "energy.py"]
                        ],
               'parents': ['1'],
               'files': {'energy.py': energy_calculation},
               'scheduler_options': {'-N': 5},
               'keep_run': True
              }
    # tasks not list
    fail[7] = {'name': 'energy calculation',
               'tasks': {},
               'files': {'energy.py': energy_calculation},
               'parents': [1, 2],
               'scheduler_options': {'-N': 5},
               'keep_run': True
              }
    # bad shell task
    fail[8] = {'name': 'energy calculation',
               'tasks': [['shell', 1],
                         ['python', "energy.py"]
                        ],
               'files': {'energy.py': energy_calculation},
               'parents': [1, 2],
               'scheduler_options': {'-N': 5},
               'keep_run': True
              }
    # bad python task
    fail[9] = {'name': 'energy calculation',
               'tasks': [['shell', "export OMP_NUM_THREADS=1"],
                         ['python', 1],
                        ],
               'files': {'energy.py': energy_calculation},
               'parents': [1, 2],
               'scheduler_options': {'-N': 5},
               'keep_run': True
              }
    # bad python task
    fail[10] = {'name': 'energy calculation',
                'tasks': [['shell', "export OMP_NUM_THREADS=1"],
                          ['python', []]
                         ],
                'files': {'energy.py': energy_calculation},
                'parents': [1, 2],
                'scheduler_options': {'-N': 5},
                'keep_run': True
               }
    # bad python task
    fail[11] = {'name': 'energy calculation',
                'tasks': [['shell', "export OMP_NUM_THREADS=1"],
                          ['python', "energy.py", {'config': 0}, 1]
                         ],
                'files': {'energy.py': energy_calculation},
                'parents': [1, 2],
                'scheduler_options': {'-N': 5},
                'keep_run': True
               }
    # bad python task
    fail[12] = {'name': 'energy calculation',
                'tasks': [['shell']
                         ],
                'files': {'energy.py': energy_calculation},
                'parents': [1, 2],
                'scheduler_options': {'-N': 5},
                'keep_run': True
               }
    # bad python task
    fail[13] = {'name': 'energy calculation',
                'tasks': ['shell'],
                'files': {'energy.py': energy_calculation},
                'parents': [1, 2],
                'scheduler_options': {'-N': 5},
                'keep_run': True
               }
    # bad python task
    fail[14] = {'name': 'energy calculation',
                'tasks': [['shell', 'echo 1']
                         ],
                'files': {1: energy_calculation},
                'parents': [1, 2],
                'scheduler_options': {'-N': 5},
                'keep_run': True
               }
    # bad python task
    fail[15] = {'name': 'energy calculation',
                'tasks': [['shell', 'echo 1']
                         ],
                'files': {'energy.py': 1},
                'parents': [1, 2],
                'scheduler_options': {'-N': 5},
                'keep_run': True
               }

    for fail_ in fail:
        run = RunnerData(fail_)
        with pytest.raises(RuntimeError):
            _ = run.get_runner_data()

