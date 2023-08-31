"""
Lazily load a collection of chips (a 'packet' of chips...).
"""

import pathlib
import typing

import xarray as xr

import ecofuture_preproc.chips


def form_packet(
    paths: list[pathlib.Path],
    fill_value: typing.Union[float, int],
) -> xr.DataArray:

    return xr.combine_by_coords(
        data_objects=[
            ecofuture_preproc.chips.read_chip(
                path=chip_path,
                chunks="auto",
                load_data=False,
            )
            for chip_path in paths
        ],
        fill_value=fill_value,
    )
