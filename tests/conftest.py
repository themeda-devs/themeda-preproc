import pytest


@pytest.fixture(scope="session", autouse=True)
def base_output_dir(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("data")
    return tmp_path
