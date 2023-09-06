import pathlib
import shutil

import ecofuture_preproc.source
import ecofuture_preproc.utils
import ecofuture_preproc.land_cover.utils


def run(
    source_name: ecofuture_preproc.source.DataSourceName,
    base_output_dir: pathlib.Path,
    protect: bool = True,
) -> None:
    raw_dir = base_output_dir / "raw" / source_name.value
    prep_dir = base_output_dir / "prep" / source_name.value

    for chip_path in raw_dir.glob("*.tif"):

        chip_year = get_year_from_chip_path(path=chip_path)

        output_dir = prep_dir / str(chip_year)
        output_dir.mkdir(exist_ok=True, parents=True)
        output_path = output_dir / f"{source_name.value}_{chip_year}.tif"

        if not ecofuture_preproc.utils.is_path_existing_and_read_only(
            path=output_path
        ):
            shutil.copy2(
                src=chip_path,
                dst=output_path,
            )

        if protect:
            ecofuture_preproc.utils.protect_path(path=output_path)


def get_year_from_chip_path(path: pathlib.Path) -> int:

    filename = path.stem

    if filename.startswith("lu"):
        year_end = filename[2:4]
        if year_end[0] == "0":
            year_start = "20"
        elif year_end[0] == "9":
            year_start = "19"
        else:
            raise ValueError("Unexpected year string")
        year = year_start + year_end

    elif filename.startswith("NLUM"):
        (_, _, _, year, _, _) = filename.split("_")

    else:
        raise ValueError("Unexpected filename structure")

    year_f = float(year)

    if not year_f.is_integer():
        raise ValueError("Unexpected year format")

    year_i = int(year_f)

    return year_i

