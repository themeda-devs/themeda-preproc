import pathlib
import types
import collections
import contextlib
import shutil

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
    roi = ecofuture_preproc.roi.RegionOfInterest(
        name=roi_name,
        base_output_dir=base_output_dir,
        load=True,
    )

    prep_dir = base_output_dir / "prep" / source_name.value
    chip_dir = base_output_dir / "chips" / f"roi_{roi_name.value}" / source_name.value

    chip_dir.mkdir(exist_ok=True, parents=True)

    prep_chip_path_info = collections.defaultdict(list)

    for year in prep_dir.glob("*"):
        if not year.is_dir():
            continue

        for chip_path in year.glob("*.tif"):
            chip_path_info = ecofuture_preproc.land_cover.utils.parse_chip_path(
                path=chip_path,
            )

            prep_chip_path_info[chip_path_info.grid_ref].append(chip_path_info)

    # make immutable
    prep_chip_path_info = types.MappingProxyType(prep_chip_path_info)

    with contextlib.closing(
        tqdm.tqdm(
            iterable=None,
            total=len(prep_chip_path_info),
            disable=not show_progress,
        )
    ) as progress_bar:
        for grid_ref_path_info in prep_chip_path_info.values():
            (representative_chip, *_) = grid_ref_path_info

            if not is_grid_ref_valid(
                grid_ref_chip_path_info=representative_chip,
                roi=roi,
            ):
                continue

            for chip_path_info in grid_ref_path_info:
                output_dir = chip_dir / str(chip_path_info.year)
                output_dir.mkdir(exist_ok=True, parents=True)
                output_path = output_dir / chip_path_info.path.name

                if not ecofuture_preproc.utils.is_path_existing_and_read_only(
                    path=output_path
                ):
                    shutil.copy2(
                        src=chip_path_info.path,
                        dst=output_path,
                    )

                if protect:
                    ecofuture_preproc.utils.protect_path(path=output_path)

            progress_bar.update()


def is_grid_ref_valid(
    grid_ref_chip_path_info: ecofuture_preproc.chips.ChipPathInfo,
    roi: ecofuture_preproc.roi.RegionOfInterest,
) -> bool:
    with contextlib.closing(
        ecofuture_preproc.chips.read_chip(
            path=grid_ref_chip_path_info.path,
            load_data=False,
        )
    ) as reference_chip:
        chip_bounds = reference_chip.odc.geobox.boundingbox.polygon.geom

        intersects: bool = chip_bounds.intersects(other=roi.shape)

    return intersects
