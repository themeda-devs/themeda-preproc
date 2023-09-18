import pathlib
import shutil

import ecofuture_preproc.source
import ecofuture_preproc.utils


def run(
    source_name: ecofuture_preproc.source.DataSourceName,
    base_output_dir: pathlib.Path,
    protect: bool = True,
) -> None:

    if source_name not in [
        ecofuture_preproc.source.DataSourceName.SOIL_ECE,
        ecofuture_preproc.source.DataSourceName.SOIL_DEPTH,
        ecofuture_preproc.source.DataSourceName.SOIL_CLAY,
    ]:
        raise ValueError("Unexpected source name")

    raw_dir = base_output_dir / "raw" / source_name.value
    prep_dir = base_output_dir / "prep" / source_name.value

    if source_name == ecofuture_preproc.source.DataSourceName.SOIL_DEPTH:
        filename = "DES_000_200_EV_N_P_AU_TRN_C_20190901.tif"
        year = 2019
    elif source_name == ecofuture_preproc.source.DataSourceName.SOIL_ECE:
        filename = "ECE_000_005_EV_N_P_AU_NAT_C_20140801.tif"
        year = 2014
    elif source_name == ecofuture_preproc.source.DataSourceName.SOIL_CLAY:
        filename = "CLY_000_005_EV_N_P_AU_TRN_N_20210902.tif"
        year = 2021

    raw_chip_path = raw_dir / filename

    if not raw_chip_path.exists():
        raise ValueError(f"Data not at expected path ({raw_chip_path})")

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
