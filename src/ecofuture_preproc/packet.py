"""
Lazily load a collection of chips (a 'packet' of chips...).
"""

import pathlib
import typing
import tempfile
import asyncio
import concurrent

import numpy.typing as npt

import xarray as xr

import polars

import ecofuture_preproc.chips
import ecofuture_preproc.chiplets


def form_packet(
    paths: list[pathlib.Path],
    fill_value: typing.Union[float, int],
    chunks: typing.Optional[typing.Union[dict[str, int], bool, str]] = "auto",
) -> xr.DataArray:

    data = read_files(paths=paths, chunks=chunks)

    data_array = xr.combine_by_coords(
        data_objects=data,
        fill_value=fill_value,
        combine_attrs="drop_conflicts",
    )

    return data_array


def read_files(
    paths: list[pathlib.Path],
    chunks: typing.Optional[typing.Union[dict[str, int], bool, str]] = "auto",
) -> list[xr.DataArray]:

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                ecofuture_preproc.chips.read_chip,
                path=path,
                chunks=chunks,
            )
            for path in paths
        ]

    return [future.result() for future in futures]
