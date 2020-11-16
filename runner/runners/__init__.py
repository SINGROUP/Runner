from runner.runners.slurm import SlurmRunner
from runner.runners.terminal import TerminalRunner


__all__ = ['SlurmRunner', 'TerminalRunner']

runner_list = ['SlurmRunner', 'TerminalRunner']
