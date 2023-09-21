import pathlib
import types

import xarray as xr

import ecofuture_preproc.chips
import ecofuture_preproc.roi


def load_reference_chips(
    base_output_dir: pathlib.Path,
    roi_name: ecofuture_preproc.roi.ROIName,
) -> types.MappingProxyType[ecofuture_preproc.chips.GridRef, xr.DataArray]:
    base_chip_dir = base_output_dir / "chips" / f"roi_{roi_name.value}" / "land_cover"

    # work out which DEA chip year to use as the reference
    (base_chip_ref_year, *_) = (
        int(base_chip_year_path.name)
        for base_chip_year_path in sorted(base_chip_dir.glob("*"))
        if base_chip_year_path.is_dir()
    )

    chip_dir = base_chip_dir / str(base_chip_ref_year)

    # load all the DEA chips
    base_chips: dict[ecofuture_preproc.chips.GridRef, xr.DataArray] = {}

    for base_chip_path in sorted(chip_dir.glob("*.tif")):
        base_chip_path_info = parse_chip_path(path=base_chip_path)

        base_chip = ecofuture_preproc.chips.read_chip(
            path=base_chip_path,
            chunks="auto",
        )

        if base_chip_path_info.grid_ref in base_chips:
            raise ValueError("Unexpected grid ref duplication")

        base_chips[base_chip_path_info.grid_ref] = base_chip

    base_chips = types.MappingProxyType(base_chips)

    return base_chips


def parse_chip_path(path: pathlib.Path) -> ecofuture_preproc.chips.ChipPathInfo:
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
