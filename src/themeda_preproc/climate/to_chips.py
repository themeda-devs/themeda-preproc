import pathlib
import contextlib

import xarray as xr

import odc.geo.xr  # noqa

import tqdm

import themeda_preproc.source
import themeda_preproc.roi
import themeda_preproc.utils
import themeda_preproc.land_cover.utils


def run(
    source_name: themeda_preproc.source.DataSourceName,
    roi_name: themeda_preproc.roi.ROIName,
    base_output_dir: pathlib.Path,
    protect: bool = True,
    show_progress: bool = True,
) -> None:
    prep_dir = base_output_dir / "prep" / source_name.value

    chip_dir = base_output_dir / "chips" / f"roi_{roi_name.value}" / source_name.value
    chip_dir.mkdir(exist_ok=True, parents=True)

    # load all the DEA chips
    ref_chips = themeda_preproc.land_cover.utils.load_reference_chips(
        base_output_dir=base_output_dir,
        roi_name=roi_name,
    )

    years = themeda_preproc.utils.get_years_in_path(path=prep_dir)

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

            climate_chip_path = prep_dir / str(year) / f"{source_name.value}_{year}.tif"

            if not climate_chip_path.exists():
                raise ValueError(
                    f"Expected the climate prep chip to exist at {climate_chip_path}"
                )

            # load the climate chip
            climate_chip = themeda_preproc.chips.read_chip(
                path=climate_chip_path,
                load_data=True,
                masked=True,
            )

            for grid_ref, base_chip in ref_chips.items():
                output_path = (
                    year_chip_dir
                    / f"{source_name.value}_roi_{roi_name.value}_{year}_{grid_ref}.tif"
                )

                if not themeda_preproc.utils.is_path_existing_and_read_only(
                    path=output_path
                ):
                    converted_chip = convert_chip(
                        climate_chip=climate_chip,
                        dea_chip=base_chip,
                        source_name=source_name,
                    )

                    converted_chip.rio.to_raster(
                        raster_path=output_path,
                        compress="lzw",
                    )

                    if protect:
                        themeda_preproc.utils.protect_path(path=output_path)

                progress_bar.update()


def convert_chip(
    climate_chip: xr.DataArray,
    dea_chip: xr.DataArray,
    source_name: themeda_preproc.source.DataSourceName,
) -> xr.DataArray:
    interp_method = themeda_preproc.source.DATA_SOURCE_RESAMPLERS[source_name]

    climate_chip_resampled = climate_chip.odc.reproject(
        how=dea_chip.odc.geobox,
        resampling=interp_method,
    ).compute()

    climate_chip_resampled.rio.set_crs(input_crs=dea_chip.rio.crs, inplace=True)

    nodata_val = themeda_preproc.source.DATA_SOURCE_NODATA[source_name]
    climate_chip_resampled.rio.set_nodata(input_nodata=nodata_val, inplace=True)

    return climate_chip_resampled
