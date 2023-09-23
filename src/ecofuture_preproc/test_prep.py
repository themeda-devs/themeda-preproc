
import pathlib
import random
import shutil

import ecofuture_preproc.utils
import ecofuture_preproc.source
import ecofuture_preproc.roi
import ecofuture_preproc.chips


def run(
    roi_name: ecofuture_preproc.roi.ROIName,
    base_output_dir: pathlib.Path,
) -> None:

    (_, _, repo_base_path, *_) = pathlib.Path(__file__).parents

    test_data_path = repo_base_path / "test_data"
    test_data_path.mkdir(exist_ok=True)

    test_grid_refs = [
        ecofuture_preproc.chips.GridRef(x=x, y=y)
        for (x, y) in [
            (2, -14), (2, -13), (3, -14), (3, -13)
        ]
    ]

    start_rand_state = random.getstate()

    try:

        random.seed(1048907938)

        for source_name in ecofuture_preproc.source.DataSourceName:

            source_chip_path = (
                base_output_dir
                / "chips"
                / f"roi_{roi_name.value}"
                / source_name.value
            )

            if not source_chip_path.exists():
                raise ValueError(f"Expected path at {source_chip_path}")

            years = ecofuture_preproc.utils.get_years_in_path(path=source_chip_path)

            year = random.choice(years)

            source_chip_path /= str(year)

            test_output_path = (
                test_data_path
                / "chips"
                / f"roi_{roi_name.value}"
                / source_name.value
                / str(year)
            )

            test_output_path.mkdir(exist_ok=True, parents=True)

            for test_grid_ref in test_grid_refs:

                (src_path,) = source_chip_path.glob(f"*{test_grid_ref}*.tif")

                dst_path = test_output_path / src_path.name

                shutil.copy2(
                    src=src_path,
                    dst=dst_path,
                )

    finally:
        random.setstate(start_rand_state)
