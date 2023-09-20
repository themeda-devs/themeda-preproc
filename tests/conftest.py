import pathlib

import pytest


@pytest.fixture(scope="session", autouse=True)
def base_output_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("data")
