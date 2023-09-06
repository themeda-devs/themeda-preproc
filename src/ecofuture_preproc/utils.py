import pathlib
import typing
import os

import requests


def is_path_existing_and_read_only(path: pathlib.Path) -> bool:
    return (
        path.exists()
        and (not os.access(path, os.W_OK))
    )


def num_str_to_int(num_str: str) -> int:
    num = float(num_str)
    if not num.is_integer():
        raise ValueError(f"{num_str} is not an integer")
    return int(num)


def protect_path(
    path: pathlib.Path,
    permissions: int = 0o440,
) -> None:
    path.chmod(mode=permissions)


def download_file(
    url: str,
    output_path: typing.Union[pathlib.Path, str],
    overwrite: bool = True,
) -> None:
    """
    Downloads a file from a URL, raising a `ValueError` if there is a problem.
    """

    output_path = pathlib.Path(output_path)

    if output_path.exists() and not overwrite:
        raise FileExistsError(f"Output file {output_path} already exists")

    response = requests.get(url=url, stream=True)

    if not response.ok:
        raise ValueError(
            f"Error downloading {url}; response code was {response.status_code}"
        )

    with output_path.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            handle.write(chunk)
