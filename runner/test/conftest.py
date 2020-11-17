"""conftests for pytest"""

from pathlib import Path

import pytest

from runner.utils import Cd


@pytest.fixture(autouse=True)
def use_tmp_workdir(tmp_path):
    """makes temporary work directory for tests"""
    # Pytest can on some systems provide a Path from pathlib2.  Normalize:
    path = Path(str(tmp_path))
    with Cd(path, mkdir=True):
        yield tmp_path
    # We print the path so user can see where test failed, if it failed.
    print(f'Testpath: {path}')
