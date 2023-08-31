import dataclasses
import warnings
import pathlib
import typing

import xarray as xr

import odc.geo.xr  # noqa

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
    chunks: typing.Optional[typing.Union[dict[str, int], bool, str]] = None,
    variable: typing.Optional[typing.Union[str, tuple[str, ...]]] = None,
    masked: bool = False,
) -> xr.DataArray:
    "Reads a chip file from disk"

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=DeprecationWarning)
        handle = rioxarray.open_rasterio(
            filename=path,
            chunks=chunks,
            variable=variable,
            masked=masked,
        )

    if not isinstance(handle, xr.DataArray):
        raise ValueError("Unexpected data format")

    if "band" in handle.dims:
        # has a redundant `band` dimension
        handle = handle.squeeze(dim="band", drop=True)

    if load_data:
        handle.load()

    return handle


def get_grid_ref_from_chip(chip: xr.DataArray) -> GridRef:
    scale_factor = 100_000
    bbox = chip.odc.geobox.boundingbox
    x = bbox.left / scale_factor
    y = bbox.bottom / scale_factor

    if not x.is_integer() and y.is_integer():
        raise ValueError("Unexpected values")

    x = int(x)
    y = int(y)

    return GridRef(x=x, y=y)
