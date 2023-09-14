"""
Lazily load a collection of chips (a 'packet' of chips...).
"""

import pathlib
import typing
import concurrent.futures

import xarray as xr

import rioxarray.merge

import ecofuture_preproc.chips
import ecofuture_preproc.chiplets


def form_packet(
    paths: list[pathlib.Path],
    chunks: typing.Optional[typing.Union[dict[str, int], bool, str]] = "auto",
    new_resolution: typing.Optional[typing.Union[int, float]] = None,
) -> xr.DataArray:

    if new_resolution is not None:
        res = (new_resolution,) * 2
    else:
        res = None

    data = read_files(paths=paths, chunks=chunks)

    data_array = rioxarray.merge.merge_arrays(
        dataarrays=data,
        res=res,
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
