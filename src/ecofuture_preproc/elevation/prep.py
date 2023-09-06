import pathlib
import shutil

import ecofuture_preproc.source
import ecofuture_preproc.utils


def run(
    source_name: ecofuture_preproc.source.DataSourceName,
    base_output_dir: pathlib.Path,
    protect: bool = True,
) -> None:
    raw_dir = base_output_dir / "raw" / source_name.value
    prep_dir = base_output_dir / "prep" / source_name.value

    raw_chip_path = raw_dir / "srtm-1sec-demh-v1-COG.tif"

    if not raw_chip_path.exists():
        raise ValueError(f"Elevation data not at expected path ({raw_chip_path})")

    # assigning a year is a bit odd for this data, but for consistency with other
    # sources, going with the year it was published
    year = 2011

    output_dir = prep_dir / str(year)
    output_dir.mkdir(exist_ok=True, parents=True)
    output_path = output_dir / f"{source_name.value}_{year}.tif"

    if not ecofuture_preproc.utils.is_path_existing_and_read_only(
        path=output_path
    ):

        shutil.copy2(
            src=raw_chip_path,
            dst=output_path,
        )

        if protect:
            ecofuture_preproc.utils.protect_path(path=output_path)
