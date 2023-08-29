import dataclasses
import warnings
import pathlib

import xarray as xr

import rioxarray


@dataclasses.dataclass(frozen=True)
class GridRef:
    x: int
    y: int


@dataclasses.dataclass
class ChipPathInfo:
    grid_ref: GridRef
    year: int
    path: pathlib.Path


def read_chip(
    path: pathlib.Path,
    load_data: bool = False,
) -> xr.DataArray:
    "Reads a chip file from disk"

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=DeprecationWarning)
        handle = rioxarray.open_rasterio(filename=path)

    if not isinstance(handle, xr.DataArray):
        raise ValueError("Unexpected data format")

    if "band" in handle.dims:
        # has a redundant `band` dimension
        handle = handle.squeeze(dim="band", drop=True)

    if load_data:
        handle.load()

    return handle
