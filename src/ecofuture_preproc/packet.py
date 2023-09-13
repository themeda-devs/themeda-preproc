"""
Lazily load a collection of chips (a 'packet' of chips...).
"""

import pathlib
import typing
import tempfile

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
    return xr.combine_by_coords(
        data_objects=[
            ecofuture_preproc.chips.read_chip(
                path=chip_path,
                chunks=chunks,
                load_data=False,
            )
            for chip_path in paths
        ],
        fill_value=fill_value,
        combine_attrs="drop_conflicts",
    )


def form_packet_from_chiplets(
    chiplets: npt.NDArray,
    table: polars.dataframe.frame.DataFrame,
    fill_value: typing.Union[int, float],
    pad_size_pix: int,
    base_size_pix: int = 160,
    crs: int = 3577,
    chunks: typing.Optional[typing.Union[dict[str, int], bool, str]] = "auto",
    new_resolution: typing.Optional[typing.Union[int, float]] = None,
) -> xr.DataArray:

    data_arrays: list[xr.DataArray] = []

    for entry in table.iter_rows(named=True):

        chiplet = chiplets[entry["index"], ...]

        data_array = ecofuture_preproc.chiplets.convert_chiplet_to_data_array(
            chiplet=chiplet,
            metadata=entry,
            pad_size_pix=pad_size_pix,
            base_size_pix=base_size_pix,
            crs=crs,
            nodata=fill_value,
            new_resolution=new_resolution,
        )

        if chunks is not None:

            # doesn't seem right, but I don't know a better way of doing this!

            try:
                temp_file = tempfile.NamedTemporaryFile(suffix=".tif")

                data_array.rio.to_raster(raster_path=temp_file.name)

                data_array.close()

                data_array = ecofuture_preproc.chips.read_chip(
                    path=temp_file.name,
                    chunks=chunks,
                )

            finally:
                temp_file.close()

        data_arrays.append(data_array)

    return data_arrays

    packet = xr.combine_by_coords(
        data_objects=data_arrays,
        fill_value=fill_value,
    )

    return packet
