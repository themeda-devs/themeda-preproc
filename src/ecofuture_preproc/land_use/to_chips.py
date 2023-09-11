import pathlib
import contextlib

import numpy as np

import xarray as xr

import rasterio

import odc.geo.xr  # noqa

import tqdm

import ecofuture_preproc.source
import ecofuture_preproc.roi
import ecofuture_preproc.utils
import ecofuture_preproc.source
import ecofuture_preproc.land_cover.utils


def run(
    source_name: ecofuture_preproc.source.DataSourceName,
    roi_name: ecofuture_preproc.roi.ROIName,
    base_output_dir: pathlib.Path,
    protect: bool = True,
    show_progress: bool = True,
) -> None:

    prep_dir = base_output_dir / "prep" / source_name.value

    chip_dir = base_output_dir / "chips" / f"roi_{roi_name.value}" / source_name.value
    chip_dir.mkdir(exist_ok=True, parents=True)

    # load all the DEA chips
    ref_chips = ecofuture_preproc.land_cover.utils.load_reference_chips(
        base_output_dir=base_output_dir,
        roi_name=roi_name,
    )

    years = sorted(
        [
            int(prep_year_dir.name)
            for prep_year_dir in prep_dir.glob("*")
            if prep_year_dir.is_dir() and len(prep_year_dir.name) == 4
        ]
    )

    n_total_conversions = len(years) * len(ref_chips)

    with contextlib.closing(
        tqdm.tqdm(
            iterable=None,
            total=n_total_conversions,
            disable=not show_progress,
        )
    ) as progress_bar:

        for year in years:

            year_chip_dir = chip_dir / str(year)
            year_chip_dir.mkdir(exist_ok=True, parents=True)

            land_use_chip_path = (
                prep_dir
                / str(year)
                / f"{source_name.value}_{year}.tif"
            )

            if not land_use_chip_path.exists():
                raise ValueError(
                    f"Expected the prep chip to exist at {land_use_chip_path}"
                )

            # load the climate chip
            land_use_chip = ecofuture_preproc.chips.read_chip(
                path=land_use_chip_path,
                masked=True,
            )

            for (grid_ref, base_chip) in ref_chips.items():

                output_path = (
                    year_chip_dir
                    / f"{source_name.value}_roi_{roi_name.value}_{year}_{grid_ref}.tif"
                )

                if not ecofuture_preproc.utils.is_path_existing_and_read_only(
                    path=output_path
                ):

                    converted_chip = convert_chip(
                        land_use_chip=land_use_chip,
                        dea_chip=base_chip,
                    )

                    converted_chip.rio.to_raster(
                        raster_path=output_path,
                        compress="lzw",
                    )

                    if protect:
                        ecofuture_preproc.utils.protect_path(path=output_path)

                progress_bar.update()


def convert_chip(
    land_use_chip: xr.DataArray,
    dea_chip: xr.DataArray,
) -> xr.DataArray:

    source_name = ecofuture_preproc.source.DataSourceName("land_use")
    nodata_val = ecofuture_preproc.source.DATA_SOURCE_NODATA[source_name]

    interp_method = rasterio.enums.Resampling.nearest

    land_use_chip_resampled = land_use_chip.odc.reproject(
        how=dea_chip.odc.geobox,
        resampling=interp_method,
    ).compute()

    land_use_chip_resampled = xr.where(
        cond=land_use_chip_resampled.isnull(),
        x=nodata_val,
        y=land_use_chip_resampled,
    )

    land_use_chip_resampled = land_use_chip_resampled.astype(np.uint16)

    land_use_chip_resampled.rio.set_crs(input_crs=dea_chip.rio.crs, inplace=True)
    land_use_chip_resampled.rio.set_nodata(input_nodata=nodata_val, inplace=True)

    return land_use_chip_resampled
