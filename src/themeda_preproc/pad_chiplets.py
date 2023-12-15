import typing
import contextlib
import pathlib

import numpy as np
import numpy.typing as npt

import xarray as xr

import polars as pl

import tqdm

import affine

import rioxarray.merge

import themeda_preproc.roi
import themeda_preproc.chips
import themeda_preproc.chiplet_table
import themeda_preproc.chiplets


def run(
    base_output_dir: pathlib.Path,
    chiplets_path: pathlib.Path,
    output_path: pathlib.Path,
    dtype: npt.DTypeLike,
    roi_name: themeda_preproc.roi.ROIName,
    base_size_pix: int,
    pad_size_pix: int,
    n_in_extra_dim: int = 0,
    protect: bool = True,
    show_progress: bool = True,
) -> None:
    # if it already exists and is protected, bail
    if themeda_preproc.utils.is_path_existing_and_read_only(path=output_path):
        print(f"Path {output_path} exists and is protected; skipping")
        return

    # load the chiplet metadata for the unpadded and padded data
    (nopad_table, pad_table) = (
        themeda_preproc.chiplet_table.load_table(
            roi_name=roi_name,
            base_output_dir=base_output_dir,
            pad_size_pix=curr_pad_size_pix,
        )
        for curr_pad_size_pix in [0, pad_size_pix]
    )

    # load the chiplets as a mmap array
    chiplets = load_chiplets(
        chiplets_path=chiplets_path,
        chiplet_dtype=dtype,
        chiplet_table=nopad_table,
        chiplet_n_in_extra_dim=n_in_extra_dim,
        base_size_pix=base_size_pix,
        pad_size_pix=0,
    )

    # insert an empty axis if chiplets are 3D
    if chiplets.ndim == 3:
        chiplets = np.expand_dims(a=chiplets, axis=1)

    output_spatial_dim_size = base_size_pix + pad_size_pix * 2
    output_shape: typing.Union[tuple[int, int, int, int], tuple[int, int, int]]
    if n_in_extra_dim > 0:
        output_shape = (chiplets.shape[0], n_in_extra_dim) + (
            output_spatial_dim_size,
        ) * 2
    else:
        output_shape = (chiplets.shape[0],) + (output_spatial_dim_size,) * 2

    # create the output array
    padded_chiplets = np.memmap(
        filename=output_path,
        dtype=dtype,
        mode="w+",
        shape=output_shape,
    )

    nodata = 0 if dtype == np.uint8 else np.nan

    # store the transforms so we don't have to recalculate
    transforms: dict[themeda_preproc.chips.GridRef, affine.Affine] = {}

    with contextlib.closing(
        tqdm.tqdm(
            iterable=None,
            total=int(np.prod(chiplets.shape[:2])),
            disable=not show_progress,
        )
    ) as progress_bar:
        # consider each entry in the additional dim separately, because memory
        for i_extra_dim in range(chiplets.shape[1]):
            data_array = rioxarray.merge.merge_arrays(
                dataarrays=[
                    themeda_preproc.chiplets.convert_chiplet_to_data_array(
                        chiplet=chiplets[i_chiplet, i_extra_dim, :, :],
                        metadata=row,
                        pad_size_pix=0,
                        base_size_pix=base_size_pix,
                        nodata=nodata,
                    )
                    for (i_chiplet, row) in enumerate(nopad_table.iter_rows(named=True))
                ],
                nodata=nodata,
            )

            for row in pad_table.iter_rows(named=True):
                grid_ref = themeda_preproc.chips.GridRef(
                    x=row["chip_grid_ref_x_base"],
                    y=row["chip_grid_ref_y_base"],
                )

                if grid_ref not in transforms:
                    transforms[
                        grid_ref
                    ] = themeda_preproc.chiplets.get_transform_from_row(row=row)

                try:
                    padded_chiplet = themeda_preproc.chiplets.get_chiplet_from_packet(
                        packet=data_array,
                        chip_i_x_base=row["chip_i_x_base"],
                        chip_i_y_base=row["chip_i_y_base"],
                        chip_i_to_coords_transform=transforms[grid_ref],
                        base_size_pix=base_size_pix,
                        pad_size_pix=pad_size_pix,
                        method="nearest",
                    )

                except KeyError as err:
                    print("---")
                    print(err.args)
                    print(row)
                    print(grid_ref)
                    continue

                if n_in_extra_dim > 0:
                    padded_chiplets[row["index"], i_extra_dim, ...] = padded_chiplet
                else:
                    padded_chiplets[row["index"], ...] = padded_chiplet

                progress_bar.update()

    padded_chiplets.flush()
    assert hasattr(padded_chiplets, "_mmap")
    padded_chiplets._mmap.close()

    if protect:
        themeda_preproc.utils.protect_path(path=output_path)


def add_padding_to_packet(
    packet: xr.DataArray,
    pad_size_pix: int,
) -> xr.DataArray:
    x_delta = np.abs(packet.x[1] - packet.x[0]).item()
    y_delta = np.abs(packet.y[1] - packet.y[0]).item()

    # current boundaries
    (left, bottom, right, top) = packet.rio.bounds(recalc=True)

    left -= x_delta * pad_size_pix
    bottom -= y_delta * pad_size_pix
    right += x_delta * pad_size_pix
    top += y_delta * pad_size_pix

    return packet.rio.pad_box(minx=left, miny=bottom, maxx=right, maxy=top)


def load_chiplets(
    chiplets_path: pathlib.Path,
    chiplet_dtype: npt.DTypeLike,
    chiplet_table: pl.dataframe.frame.DataFrame,
    chiplet_n_in_extra_dim: int = 0,
    base_size_pix: int = 160,
    pad_size_pix: int = 0,
) -> np.memmap[typing.Any, np.dtype[typing.Any]]:
    shape = themeda_preproc.chiplets.get_array_shape(
        table=chiplet_table,
        base_size_pix=base_size_pix,
        pad_size_pix=pad_size_pix,
    )

    if chiplet_n_in_extra_dim > 0:
        (n_chiplets, n_y, n_x) = shape
        shape = (n_chiplets, chiplet_n_in_extra_dim, n_y, n_x)

    chiplets_handle: np.memmap[typing.Any, np.dtype[typing.Any]] = np.memmap(
        filename=chiplets_path,
        dtype=chiplet_dtype,
        mode="r",
        shape=shape,
    )

    return chiplets_handle
