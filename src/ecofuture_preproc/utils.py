import pathlib
import typing
import os

import numpy as np
import numpy.typing as npt

import requests

import pyproj
import shapely


def get_years_in_path(
    path: pathlib.Path,
    error_if_no_years: bool = True,
    error_if_other_files: bool = True,
) -> list[int]:
    years = []

    for entry in sorted(path.glob("*")):
        if not entry.is_dir():
            if error_if_other_files:
                raise ValueError(f"Found unexpected non-year path at {entry}")
        else:
            try:
                year = num_str_to_int(num_str=entry.name)
                years.append(year)
            except ValueError:
                if error_if_other_files:
                    raise

    if len(years) == 0 and error_if_no_years:
        raise ValueError(f"No year directories found in {path}")

    return years


def get_shape_transformer(
    src_crs: int,
    dst_crs: int,
) -> pyproj.transformer.Transformer:
    return pyproj.Transformer.from_crs(
        crs_from=src_crs,
        crs_to=dst_crs,
        always_xy=True,
    )


def transform_shape(
    src_crs: int,
    dst_crs: int,
    shape: shapely.Geometry,
    transformer: typing.Optional[pyproj.transformer.Transformer] = None,
) -> shapely.Geometry:
    if transformer is None:
        transformer = get_shape_transformer(src_crs=src_crs, dst_crs=dst_crs)

    def shapely_transform(points: npt.NDArray[float]) -> npt.NDArray[float]:
        return np.column_stack(
            transformer.transform(
                xx=points[:, 0],
                yy=points[:, 1],
                errcheck=True,
            )
        )

    return shapely.transform(geometry=shape, transformation=shapely_transform)


def is_path_existing_and_read_only(path: pathlib.Path) -> bool:
    return path.exists() and (not os.access(path, os.W_OK))


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
