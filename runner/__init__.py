"""__init__ for runner"""
from runner.runners import SlurmRunner
from runner.runners import TerminalRunner
from runner.utils.runnerdata import RunnerData


__all__ = ['SlurmRunner', 'TerminalRunner', 'RunnerData']
__version__ = '0.1'
