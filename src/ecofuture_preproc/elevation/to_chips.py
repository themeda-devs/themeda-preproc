import pathlib
import contextlib

import xarray as xr

import rasterio

import odc.geo.xr  # noqa

import tqdm

import ecofuture_preproc.source
import ecofuture_preproc.roi
import ecofuture_preproc.utils
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

    (year,) = sorted(
        [
            int(prep_year_dir.name)
            for prep_year_dir in prep_dir.glob("*")
            if prep_year_dir.is_dir() and len(prep_year_dir.name) == 4
        ]
    )

    n_total_conversions = len(ref_chips)

    with contextlib.closing(
        tqdm.tqdm(
            iterable=None,
            total=n_total_conversions,
            disable=not show_progress,
        )
    ) as progress_bar:

        chip_dir = chip_dir / str(year)
        chip_dir.mkdir(exist_ok=True, parents=True)

        dem_chip_path = (
            prep_dir
            / str(year)
            / f"{source_name.value}_{year}.tif"
        )

        if not dem_chip_path.exists():
            raise ValueError(
                f"Expected the chip to exist at {dem_chip_path}"
            )

        # load the DEM chip
        dem_chip = ecofuture_preproc.chips.read_chip(
            path=dem_chip_path,
            chunks="auto",
            masked=True,
        )

        for (grid_ref, base_chip) in ref_chips.items():

            output_path = (
                chip_dir
                / f"{source_name.value}_roi_{roi_name.value}_{year}_{grid_ref}.tif"
            )

            if not ecofuture_preproc.utils.is_path_existing_and_read_only(
                path=output_path
            ):

                converted_chip = convert_chip(
                    dem_chip=dem_chip,
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
    dem_chip: xr.DataArray,
    dea_chip: xr.DataArray,
) -> xr.DataArray:

    interp_method = rasterio.enums.Resampling.bilinear

    dem_chip_resampled = dem_chip.odc.reproject(
        how=dea_chip.odc.geobox,
        resampling=interp_method,
    ).compute()

    return dem_chip_resampled
