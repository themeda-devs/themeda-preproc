import pathlib
import shutil

import xarray as xr

import themeda_preproc.source
import themeda_preproc.utils
import themeda_preproc.land_cover.utils
import themeda_preproc.land_use.labels


def run(
    source_name: themeda_preproc.source.DataSourceName,
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

        if not themeda_preproc.utils.is_path_existing_and_read_only(path=output_path):
            # for the files between 1992 and 2005, we have had to export from
            # ArcCatalog, which doesn't exported the land use codes we are after
            # we need to convert from the 'value' stored in the tif file into
            # the land use code based on a LUT
            if chip_path.name.startswith("lu"):
                chip = convert_chip_value_to_land_use_code(chip_path=chip_path)
                chip.rio.to_raster(
                    raster_path=output_path,
                    compress="lzw",
                )

            # otherwise, we can just copy the file
            else:
                shutil.copy2(
                    src=chip_path,
                    dst=output_path,
                )

            if protect:
                themeda_preproc.utils.protect_path(path=output_path)


def convert_chip_value_to_land_use_code(chip_path: pathlib.Path) -> xr.DataArray:
    lu_code_lut = themeda_preproc.land_use.labels.get_lu_code_lut(
        attr_table_path=chip_path.with_suffix(".txt")
    )

    chip = themeda_preproc.chips.read_chip(path=chip_path)

    chip_converted = themeda_preproc.land_use.labels.relabel_chip(
        chip=chip,
        relabel_lut=lu_code_lut,
    )

    return chip_converted


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
