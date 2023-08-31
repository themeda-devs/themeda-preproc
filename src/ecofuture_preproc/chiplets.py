import pathlib
import typing
import os
import multiprocessing
import functools
import contextlib

import numpy as np

import xarray as xr

import polars as pl

import affine

import tqdm

import ecofuture_preproc.source
import ecofuture_preproc.roi
import ecofuture_preproc.chips
import ecofuture_preproc.packet


def form_chiplets(
    #table: pl.dataframe.frame.DataFrame,
    source_name: ecofuture_preproc.source.DataSourceName,
    roi: ecofuture_preproc.roi.RegionOfInterest,
    base_size_pix: int,
    pad_size_pix: int,
    base_output_dir: pathlib.Path,
    protect: bool,
    cores: int,
    show_progress: bool = True,
) -> None:

    source_chip_dir = (
        base_output_dir
        / "chips"
        / f"roi_{roi.name.value}"
        / source_name.value
    )

    years = [
        int(year_path.name)
        for year_path in sorted(source_chip_dir.glob("*"))
        if year_path.is_dir()
    ]

    roi_name = ecofuture_preproc.roi.ROIName(roi.name)

    table = ecofuture_preproc.chiplet_table.load_table(
        roi_name=roi_name,
        base_output_dir=base_output_dir,
        pad_size_pix=pad_size_pix,
    )[:5]

    func = functools.partial(
        form_year_chiplets,
        table=table,
        source_name=source_name,
        roi=roi,
        base_size_pix=base_size_pix,
        pad_size_pix=pad_size_pix,
        base_output_dir=base_output_dir,
        protect=protect,
        show_progress=show_progress,
    )

    with multiprocessing.Pool(processes=cores) as pool:
        pool.starmap(func, enumerate(years))


def form_year_chiplets(
    progress_bar_position: int,
    year: int,
    table: pl.dataframe.frame.DataFrame,
    source_name: ecofuture_preproc.source.DataSourceName,
    roi: ecofuture_preproc.roi.RegionOfInterest,
    base_size_pix: int,
    pad_size_pix: int,
    base_output_dir: pathlib.Path,
    protect: bool,
    show_progress: bool,
) -> None:

    with contextlib.closing(
        tqdm.tqdm(
            iterable=None,
            total=5,
            disable=not show_progress,
            position=progress_bar_position,
            desc=str(year),
        )
    ) as progress_bar:

        chip_dir = (
            base_output_dir
            / "chips"
            / f"roi_{roi.name.value}"
            / source_name.value
            / str(year)
        )

        chip_paths = sorted(chip_dir.glob("*.tif"))

        if len(chip_paths) == 0:
            raise ValueError(f"No chips found in {chip_dir}")

        output_path = get_chiplet_path(
            source_name=source_name,
            year=year,
            roi_name=ecofuture_preproc.roi.ROIName(roi.name),
            pad_size_pix=pad_size_pix,
            base_output_dir=base_output_dir,
        )

        if output_path.exists() and not os.access(output_path, os.W_OK):
            print(f"Path {output_path} exists and is protected; skipping")
            return

        packet = ecofuture_preproc.packet.form_packet(
            paths=chip_paths,
            fill_value=ecofuture_preproc.source.DATA_SOURCE_NODATA[source_name],
        )

        chiplets = np.memmap(
            filename=output_path,
            dtype=ecofuture_preproc.source.DATA_SOURCE_DTYPE[source_name],
            mode="w+",
            shape=get_array_shape(
                table=table,
                base_size_pix=base_size_pix,
                pad_size_pix=pad_size_pix,
            ),
        )

        transforms: dict[ecofuture_preproc.chips.GridRef, affine.Affine] = {}

        for row in table.iter_rows(named=True):

            grid_ref = ecofuture_preproc.chips.GridRef(
                x=row["chip_grid_ref_x_base"],
                y=row["chip_grid_ref_y_base"],
            )

            if grid_ref not in transforms:
                transforms[grid_ref] = get_transform_from_row(row=row)

            chiplet = get_chiplet_from_packet(
                packet=packet,
                chip_i_x_base=row["chip_i_x_base"],
                chip_i_y_base=row["chip_i_y_base"],
                chip_i_to_coords_transform=transforms[grid_ref],
                base_size_pix=base_size_pix,
                pad_size_pix=pad_size_pix,
            )

            chiplets[row["index"], ...] = chiplet.values

            progress_bar.update()

        # write changes to disk
        chiplets.flush()

        # close the handle
        # see https://github.com/numpy/numpy/issues/13510
        chiplets._mmap.close()  # type: ignore

        if protect:
            ecofuture_preproc.utils.protect_path(path=output_path)


def load_chiplets(
    table: pl.dataframe.frame.DataFrame,
    source_name: ecofuture_preproc.source.DataSourceName,
    year: int,
    roi_name: ecofuture_preproc.roi.ROIName,
    base_size_pix: int,
    pad_size_pix: int,
    base_output_dir: pathlib.Path,
) -> np.memmap:  # type: ignore

    chiplet_path = get_chiplet_path(
        source_name=source_name,
        year=year,
        roi_name=roi_name,
        pad_size_pix=pad_size_pix,
        base_output_dir=base_output_dir,
    )

    chiplet = np.memmap(
        filename=chiplet_path,
        dtype=ecofuture_preproc.source.DATA_SOURCE_DTYPE[source_name],
        mode="r",
        shape=get_array_shape(
            table=table,
            base_size_pix=base_size_pix,
            pad_size_pix=pad_size_pix,
        ),
    )

    return chiplet


def get_array_shape(
    table: pl.dataframe.frame.DataFrame,
    base_size_pix: int,
    pad_size_pix: int,
) -> tuple[int, int, int]:

    shape = (len(table),) + ((base_size_pix + pad_size_pix * 2),) * 2

    return shape


def get_chiplet_path(
    source_name: ecofuture_preproc.source.DataSourceName,
    year: int,
    roi_name: ecofuture_preproc.roi.ROIName,
    pad_size_pix: int,
    base_output_dir: pathlib.Path,
) -> pathlib.Path:

    chiplet_dir: pathlib.Path = (
        base_output_dir
        / "chiplets"
        / f"roi_{roi_name.value}"
        / f"pad_{pad_size_pix}"
        / source_name.value
    )

    chiplet_dir.mkdir(exist_ok=True, parents=True)

    chiplet_path = (
        chiplet_dir
        / (
            f"chiplets_{source_name.value}_{year}_"
            + f"roi_{roi_name.value}_pad_{pad_size_pix}.npy"
        )
    )

    return chiplet_path



def get_transform_from_row(row: dict[str, typing.Any]) -> affine.Affine:

    return affine.Affine(
        **{
            letter: row[f"chip_transform_i_to_coords_coeff_{letter}"]
            for letter in "abcdefghi"
        }
    )


def get_chiplet_from_packet(
    packet: xr.DataArray,
    chip_i_x_base: int,
    chip_i_y_base: int,
    chip_i_to_coords_transform: affine.Affine,
    base_size_pix: int,
    pad_size_pix: int,
) -> xr.DataArray:
    i_x = (
        np.arange(
            chip_i_x_base - pad_size_pix,
            chip_i_x_base + base_size_pix + pad_size_pix,
        )
        + 0.5
    )
    i_y = (
        np.arange(
            chip_i_y_base - pad_size_pix,
            chip_i_y_base + base_size_pix + pad_size_pix,
        )
        + 0.5
    )

    i_xy = np.row_stack((i_x, i_y))

    (x, y) = chip_i_to_coords_transform * i_xy

    chiplet = packet.sel(x=x, y=y)

    return chiplet
