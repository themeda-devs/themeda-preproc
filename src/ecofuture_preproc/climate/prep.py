import pathlib
import types
import collections
import contextlib
import shutil
import os
import dataclasses

import tqdm

import ecofuture_preproc.source
import ecofuture_preproc.utils
import ecofuture_preproc.land_cover.utils


@dataclasses.dataclass(frozen=True)
class ChipPathInfo:
    source_name: ecofuture_preproc.source.DataSourceName
    path: pathlib.Path
    year: int
    month: str


def run(
    source_name: ecofuture_preproc.source.DataSourceName,
    base_output_dir: pathlib.Path,
    protect: bool = True,
    show_progress: bool = True,
) -> None:
    raw_dir = base_output_dir / "raw" / source_name.value
    prep_dir = base_output_dir / "prep" / source_name.value

    raw_chip_path_info = collections.defaultdict(list)

    for chip_path in raw_dir.glob("*.nc"):
        chip_path_info = parse_chip_path(path=chip_path)

        raw_chip_path_info[chip_path_info.year].append(chip_path_info)

    # make immutable
    raw_chip_path_info = types.MappingProxyType(raw_chip_path_info)

    with contextlib.closing(
        tqdm.tqdm(
            iterable=None,
            total=len(raw_chip_path_info),
            disable=not show_progress,
        )
    ) as progress_bar:
        for year_raw_path_info in raw_chip_path_info.values():

            if not is_year_valid(year_raw_chip_path_info=year_raw_path_info):
                continue

            # work out which year we're dealing with
            (year,) = {chip_path_info.year for chip_path_info in year_raw_path_info}

            # TODO TODO TODO

            for chip_path_info in grid_ref_raw_path_info:
                output_dir = prep_dir / str(chip_path_info.year)

                output_dir.mkdir(exist_ok=True, parents=True)

                output_path = output_dir / chip_path_info.path.name

                exists_and_read_only = (
                    output_path.exists()
                    and (not os.access(output_path, os.W_OK))
                )

                if not exists_and_read_only:
                    shutil.copy2(
                        src=chip_path_info.path,
                        dst=output_path,
                    )

                if protect:
                    ecofuture_preproc.utils.protect_path(path=output_path)

            progress_bar.update()


def is_year_valid(year_raw_chip_path_info: list[ChipPathInfo]) -> bool:

    months = sorted([chip_path_info.month for chip_path_info in year_raw_chip_path_info])

    valid_year = months == list(range(1, 12 + 1))

    return valid_year


def parse_chip_path(path: pathlib.Path) -> ChipPathInfo:

    (head, _, source_name, monthly, yearmonth) = path.stem.split("_")

    if head != "ANUClimate" or monthly != "monthly" or path.suffix != ".nc":
        raise ValueError(f"Unexpected path: {path}")

    (year, month) = map(int, [yearmonth[:4], yearmonth[-2:]])

    return ChipPathInfo(
        source_name=ecofuture_preproc.source.DataSourceName(source_name),
        path=path,
        year=year,
        month=month,
    )
