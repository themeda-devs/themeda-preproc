import pathlib
import contextlib
import pickle

import xarray as xr

import rioxarray.merge
import rasterio

import odc.geo.xr
import odc.geo.geom

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

            geom_path = (
                prep_dir
                / str(year)
                / f"{source_name.value}_{year}.pkl"
            )

            if not geom_path.exists():
                raise ValueError(
                    f"Expected the prep geom to exist at {geom_path}"
                )

            # load the fire scar geometries
            with geom_path.open("rb") as handle:
                geoms = pickle.load(handle)

            for (grid_ref, base_chip) in ref_chips.items():

                output_path = (
                    year_chip_dir
                    / f"{source_name.value}_roi_{roi_name.value}_{year}_{grid_ref}.tif"
                )

                if not ecofuture_preproc.utils.is_path_existing_and_read_only(
                    path=output_path
                ):

                    chip = convert_to_chip(
                        geoms=geoms,
                        dea_chip=base_chip,
                    )

                    chip.rio.to_raster(
                        raster_path=output_path,
                        compress="lzw",
                    )

                    if protect:
                        ecofuture_preproc.utils.protect_path(path=output_path)

                progress_bar.update()


def convert_to_chip(
    geoms: list[odc.geo.geom.Geometry],
    dea_chip: xr.DataArray,
) -> xr.DataArray:

    chip_bbox = dea_chip.odc.geobox.boundingbox.polygon
    chip_geobox = dea_chip.odc.geobox

    relevant_geoms = [
        geom
        for geom in geoms
        if geom.intersects(chip_bbox)
    ]

    raster = xr.zeros_like(other=dea_chip)

    for geom in relevant_geoms:

        # slower, but doesn't require potentially lots of RAM to hold the
        # individual geom rasters
        raster = rioxarray.merge.merge_arrays(
            dataarrays=(
                raster,
                odc.geo.xr.rasterize(
                    poly=geom,
                    how=chip_geobox,
                ).astype(int),
            ),
            method=rasterio.merge.copy_sum,
        )

    return raster
