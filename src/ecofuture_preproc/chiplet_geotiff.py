import pathlib
import functools
import multiprocessing

import numpy as np

import xarray as xr

import rioxarray.merge

import polars as pl

import tqdm


import ecofuture_preproc.roi
import ecofuture_preproc.chiplets
import ecofuture_preproc.chiplet_table
import ecofuture_preproc.source


def run(
    source_name: ecofuture_preproc.source.DataSourceName,
    roi_name: ecofuture_preproc.roi.ROIName,
    base_output_dir: pathlib.Path,
    cores: int,
    base_size_pix: int = 160,
    protect: bool = True,
    show_progress: bool = True,
) -> None:

    pad_size_pix = 0

    table = ecofuture_preproc.chiplet_table.load_table(
        roi_name=roi_name,
        base_output_dir=base_output_dir,
        pad_size_pix=pad_size_pix,
    )

    chiplet_base_dir = (
        base_output_dir
        / "chiplets"
        / f"roi_{roi_name.value}"
        / f"pad_{pad_size_pix}"
        / source_name.value
    )

    chiplets_file_info = [
        ecofuture_preproc.chiplets.parse_chiplet_filename(filename=chiplet_path)
        for chiplet_path in sorted(chiplet_base_dir.glob("*.npy"))
    ]

    years = sorted([chiplet_file_info.year for chiplet_file_info in chiplets_file_info])

    func = functools.partial(
        convert_year_chiplets,
        base_output_dir=base_output_dir,
        source_name=source_name,
        roi_name=roi_name,
        table=table,
        base_size_pix=base_size_pix,
        protect=protect,
        show_progress=show_progress,
    )

    # see https://pola-rs.github.io/polars-book/user-guide/misc/multiprocessing/
    mp = multiprocessing.get_context(method="spawn")

    with mp.Pool(processes=cores) as pool:
        pool.starmap(func, enumerate(years), chunksize=1)


def convert_year_chiplets(
    progress_bar_position: int,
    year: int,
    base_output_dir: pathlib.Path,
    source_name: ecofuture_preproc.source.DataSourceName,
    roi_name: ecofuture_preproc.roi.ROIName,
    table: pl.dataframe.frame.DataFrame,
    base_size_pix: int,
    protect: bool,
    show_progress: bool,
) -> None:

    pad_size_pix = 0
    crs = 3577
    nodata = ecofuture_preproc.source.DATA_SOURCE_NODATA[source_name]

    chiplets = ecofuture_preproc.chiplets.load_chiplets(
        source_name=source_name,
        year=year,
        roi_name=roi_name,
        base_size_pix=base_size_pix,
        pad_size_pix=pad_size_pix,
        base_output_dir=base_output_dir,
    )

    chip_xys = table.select(["chip_grid_ref_x_base", "chip_grid_ref_y_base"]).unique()

    progress_bar = tqdm.tqdm(
        iterable=None,
        total=len(chip_xys),
        disable=not show_progress,
        position=progress_bar_position,
        desc=str(year),
        leave=True,
    )

    for chip_xy_row in chip_xys.iter_rows(named=True):

        table_rows = table.filter(
            (pl.col("chip_grid_ref_x_base") == chip_xy_row["chip_grid_ref_x_base"])
            & (pl.col("chip_grid_ref_y_base") == chip_xy_row["chip_grid_ref_y_base"])
        )

        output_path = get_chiplet_geotiff_path(
            source_name=source_name,
            year=year,
            chip_x=chip_xy_row["chip_grid_ref_x_base"],
            chip_y=chip_xy_row["chip_grid_ref_y_base"],
            roi_name=roi_name,
            base_output_dir=base_output_dir,
        )

        if not ecofuture_preproc.utils.is_path_existing_and_read_only(
            path=output_path
        ):

            data_arrays = []

            for row in table_rows.iter_rows(named=True):

                chiplet = chiplets[row["index"], ...]

                data_array = ecofuture_preproc.chiplets.convert_chiplet_to_data_array(
                    chiplet=chiplet,
                    metadata=row,
                    pad_size_pix=pad_size_pix,
                    base_size_pix=base_size_pix,
                    crs=crs,
                    nodata=nodata,
                )

                data_arrays.append(data_array)

            data = rioxarray.merge.merge_arrays(
                dataarrays=data_arrays,
                nodata=ecofuture_preproc.source.DATA_SOURCE_SENTINEL[source_name],
            )

            data.rio.set_crs(input_crs=crs, inplace=True)
            data.rio.set_nodata(input_nodata=nodata, inplace=True)

            data = data.odc.assign_crs(crs=data.rio.crs)

            if data.dtype == np.float16:
                data = data.astype(np.float32)

            data.rio.to_raster(
                raster_path=output_path,
                compress="lzw",
            )

            data.close()

            for data_array in data_arrays:
                data_array.close()

            if protect:
                ecofuture_preproc.utils.protect_path(path=output_path)

        progress_bar.update()


def get_chiplet_geotiff_path(
    source_name: ecofuture_preproc.source.DataSourceName,
    year: int,
    chip_x: int,
    chip_y: int,
    roi_name: ecofuture_preproc.roi.ROIName,
    base_output_dir: pathlib.Path,
) -> pathlib.Path:

    chiplet_geotiff_dir: pathlib.Path = (
        base_output_dir
        / "chiplets_geotiff"
        / f"roi_{roi_name.value}"
        / source_name.value
        / str(year)
    )

    chiplet_geotiff_dir.mkdir(exist_ok=True, parents=True)

    chiplet_geotiff_path = (
        chiplet_geotiff_dir
        / (
            f"chiplets_{source_name.value}_{year}_"
            + f"roi_{roi_name.value}_x{chip_x}y{chip_y}.tif"
        )
    )

    return chiplet_geotiff_path
