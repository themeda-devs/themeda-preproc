import pathlib

import ecofuture_preproc.chips


def parse_chip_path(
    path: pathlib.Path
) -> ecofuture_preproc.chips.ChipPathInfo:
    "Parses the metadata in a chip filename"

    # example: ga_ls_landcover_class_cyear_2_1-0-0_au_x9y-24_1993-01-01_level4

    filename = path.stem

    if not filename.startswith("ga_ls_landcover"):
        raise ValueError("Unexpected filename")

    (*_, xy_str, date, measurement) = filename.split("_")

    if measurement != "level4":
        raise ValueError("Unexpected measurement type")

    grid_ref = get_grid_ref_from_xy_str(xy_str=xy_str)

    (year, *_) = date.split("-")

    year = float(year)

    if not year.is_integer():
        raise ValueError("Unexpected year")

    year = int(year)

    chip_path_info = ecofuture_preproc.chips.ChipPathInfo(
        path=path,
        year=year,
        grid_ref=grid_ref,
    )

    return chip_path_info


def get_grid_ref_from_xy_str(xy_str: str) -> ecofuture_preproc.chips.GridRef:

    if not xy_str.startswith("x") or "y" not in xy_str:
        raise ValueError("Unexpected filename format")

    x = float(xy_str[1 : xy_str.index("y")])
    y = float(xy_str[xy_str.index("y") + 1 :])

    if any([not x.is_integer() or not y.is_integer()]):
        raise ValueError("Grid reference is unexpectedly float")

    return ecofuture_preproc.chips.GridRef(x=int(x), y=int(y))
