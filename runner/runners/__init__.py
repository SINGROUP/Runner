from runner.runners.slurm import SlurmRunner
from runner.runners.terminal import TerminalRunner


__all__ = ["SlurmRunner", "TerminalRunner"]

runner_list = ["SlurmRunner", "TerminalRunner"]
runner_types = ["slurm", "terminal"]
runner_type2func = {"slurm": SlurmRunner, "terminal": TerminalRunner}
