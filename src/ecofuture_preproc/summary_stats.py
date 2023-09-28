"""
Calculates descriptive summary statistics for continuous data sources.
"""

import pathlib
import dataclasses
import json

import numpy as np

import welford

import tqdm

import ecofuture_preproc.source
import ecofuture_preproc.roi
import ecofuture_preproc.utils
import ecofuture_preproc.chiplets


@dataclasses.dataclass
class SummaryStats:
    source_name: str
    years: list[int]
    min_val: float
    max_val: float
    mean: float
    sd: float


def run(
    source_name: ecofuture_preproc.source.DataSourceName,
    roi_name: ecofuture_preproc.roi.ROIName,
    base_output_dir: pathlib.Path,
    protect: bool,
    show_progress: bool = True,
) -> None:
    if not ecofuture_preproc.source.is_data_source_continuous(source_name=source_name):
        raise ValueError("Only useful to run this on float data types")

    # only consider data for up to and including this year
    last_valid_year = 2018
    pad_size_pix = 0

    chiplet_base_dir = (
        base_output_dir
        / "chiplets-denan"
        / f"roi_{roi_name.value}"
        / f"pad_{pad_size_pix}"
        / source_name.value
    )

    chiplets_file_info = [
        ecofuture_preproc.chiplets.parse_chiplet_filename(filename=chiplet_path)
        for chiplet_path in sorted(chiplet_base_dir.glob("*.npy"))
    ]

    years = sorted(
        [
            chiplet_file_info.year
            for chiplet_file_info in chiplets_file_info
        ]
    )

    if len(years) == 0:
        raise ValueError(f"No chiplets found at {chiplet_base_dir}")

    if len(years) > 1:
        years = [year for year in years if year <= last_valid_year]

    output_path = get_output_path(
        source_name=source_name,
        roi_name=roi_name,
        base_output_dir=base_output_dir,
    )

    if ecofuture_preproc.utils.is_path_existing_and_read_only(path=output_path):
        return

    # helper to accumulate the mean and SD estimates
    tracker = welford.Welford()
    min_val = None
    max_val = None

    progress_bar = None

    for year in years:

        with ecofuture_preproc.chiplets.chiplets_reader(
            source_name=source_name,
            year=year,
            roi_name=roi_name,
            pad_size_pix=0,
            base_output_dir=base_output_dir,
            denan=True,
        ) as chiplets:

            (n_chiplets, _, _) = chiplets.shape

            if progress_bar is None:
                progress_bar = tqdm.tqdm(
                    iterable=None,
                    disable=not show_progress,
                    dynamic_ncols=True,
                    total=n_chiplets * len(years),
                )

            for i_chiplet in range(n_chiplets):

                data = np.array(chiplets[i_chiplet, ...]).flatten().astype(float)

                # update the min and max
                if min_val is None:
                    min_val = np.min(data)
                else:
                    min_val = min(min_val, np.min(data))

                if max_val is None:
                    max_val = np.max(data)
                else:
                    max_val = max(max_val, np.max(data))

                # now update the accumulator
                tracker.add_all(elements=data)

                progress_bar.update()

    assert progress_bar is not None

    progress_bar.close()

    assert min_val is not None
    assert max_val is not None

    stats = SummaryStats(
        source_name=source_name.value,
        years=years,
        min_val=float(min_val),
        max_val=float(max_val),
        mean=float(tracker.mean),
        sd=float(np.sqrt(tracker.var_p)),
    )

    with output_path.open("w") as handle:
        json.dump(dataclasses.asdict(stats), handle)

    if protect:
        ecofuture_preproc.utils.protect_path(path=output_path)


def load_stats(
    source_name: ecofuture_preproc.source.DataSourceName,
    roi_name: ecofuture_preproc.roi.ROIName,
    base_output_dir: pathlib.Path,
) -> SummaryStats:

    path = get_output_path(
        source_name=source_name,
        roi_name=roi_name,
        base_output_dir=base_output_dir,
    )

    data = json.loads(path.read_text())

    stats = SummaryStats(**data)

    return stats


def get_output_path(
    source_name: ecofuture_preproc.source.DataSourceName,
    roi_name: ecofuture_preproc.roi.ROIName,
    base_output_dir: pathlib.Path,
) -> pathlib.Path:

    output_dir: pathlib.Path = (
        base_output_dir
        / "summary_stats"
        / f"roi_{roi_name.value}"
    )

    output_dir.mkdir(exist_ok=True, parents=True)

    output_path = output_dir / (
        f"summary_stats_{source_name.value}_roi_{roi_name.value}.json"
    )

    return output_path


def calc_chunk_size_given_mem_budget(
    budget_gb: float,
    base_size_pix: int = 160,
) -> int:

    # 16 bit floats
    bytes_per_pixel = 2

    n_bytes_per_chiplet = base_size_pix * base_size_pix * bytes_per_pixel

    budget_bytes = (
        budget_gb
        * 1024  # MB
        * 1024  # KB
        * 1024  # B
    )

    chunk_size = int(np.floor(budget_bytes / n_bytes_per_chiplet))

    return chunk_size