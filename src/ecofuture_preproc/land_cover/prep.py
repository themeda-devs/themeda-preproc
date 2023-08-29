
import pathlib
import types
import collections
import contextlib
import shutil

import tqdm

import ecofuture_preproc.source
import ecofuture_preproc.utils
import ecofuture_preproc.land_cover.utils


def run(
    source_name: ecofuture_preproc.source.DataSourceName,
    base_output_dir: pathlib.Path,
    protect: bool = True,
    show_progress: bool = True,
) -> None:

    raw_dir = base_output_dir / "raw" / source_name.value
    prep_dir = base_output_dir / "prep" / source_name.value

    raw_chip_path_info = collections.defaultdict(list)

    for chip_path in raw_dir.glob("*.tif"):

        chip_path_info = ecofuture_preproc.land_cover.utils.parse_chip_path(
            path=chip_path,
        )

        raw_chip_path_info[chip_path_info.grid_ref].append(chip_path_info)

    # make immutable
    raw_chip_path_info = types.MappingProxyType(raw_chip_path_info)

    # work out how many years there are at each location
    n_years_counter: collections.Counter[int] = collections.Counter()

    for grid_ref_raw_chip_path_info in raw_chip_path_info.values():
        n_years_counter[len(grid_ref_raw_chip_path_info)] += 1

    # define the expected year count as the most common across the locations
    ((expected_n_years, _),) = n_years_counter.most_common(n=1)

    with contextlib.closing(
        tqdm.tqdm(
            iterable=None,
            total=len(raw_chip_path_info),
            disable=not show_progress,
        )
    ) as progress_bar:

        for grid_ref_raw_path_info in raw_chip_path_info.values():

            if not is_grid_ref_valid(
                grid_ref_raw_chip_path_info=grid_ref_raw_path_info,
                expected_n_years=expected_n_years,
            ):
                continue

            for chip_path_info in grid_ref_raw_path_info:

                output_dir = prep_dir / str(chip_path_info.year)

                output_dir.mkdir(exist_ok=True, parents=True)

                output_path = output_dir / chip_path_info.path.name

                if not output_path.exists():

                    try:
                        shutil.copy2(
                            src=chip_path_info.path,
                            dst=output_path,
                        )
                    except PermissionError:
                        pass

                if protect:
                    ecofuture_preproc.utils.protect_path(path=output_path)

            progress_bar.update()


def is_grid_ref_valid(
    grid_ref_raw_chip_path_info: list[ecofuture_preproc.chips.ChipPathInfo],
    expected_n_years: int
) -> bool:

    n_years = len(grid_ref_raw_chip_path_info)

    if n_years != expected_n_years:
        return False

    reference_chip = ecofuture_preproc.chips.read_chip(
        path=grid_ref_raw_chip_path_info[0].path,
        load_data=False,
    )

    for comparison_chip_path_info in grid_ref_raw_chip_path_info[1:]:

        with contextlib.closing(
            ecofuture_preproc.chips.read_chip(
                path=comparison_chip_path_info.path,
                load_data=False,
            )
        ) as comparison_chip:

            ok = all(
                [
                    reference_chip.x.equals(comparison_chip.x),
                    reference_chip.y.equals(comparison_chip.y),
                    reference_chip.rio.bounds() == comparison_chip.rio.bounds(),
                    reference_chip.rio.crs == comparison_chip.rio.crs,
                    reference_chip.rio.shape == comparison_chip.rio.shape,
                    reference_chip.rio.transform() == comparison_chip.rio.transform(),
                ],
            )

            if not ok:
                return False

    reference_chip.close()

    return True
