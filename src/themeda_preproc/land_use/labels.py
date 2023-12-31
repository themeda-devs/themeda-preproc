import csv
import pathlib
import importlib.resources
import typing

import numpy as np
import numpy.typing as npt

import xarray as xr

import themeda_preproc.roi
import themeda_preproc.utils
import themeda_preproc.vis.colourmap


SENTINEL_VAL: typing.Final = 99


def relabel_chiplet(
    chiplet: xr.DataArray,
    partial_roi_overlap: bool,  # noqa: ARG001
    relabel_lut: npt.NDArray[np.uint8],
    inplace: bool = True,
) -> xr.DataArray:
    if not inplace:
        chiplet = chiplet.copy()

    chiplet.data = relabel_lut[chiplet.data]

    if (chiplet == SENTINEL_VAL).any():
        raise ValueError("Unexpected relabelling; sentinel value observed")

    return chiplet


def get_relabel_lut() -> npt.NDArray[np.uint8]:
    csv_path = pathlib.Path(
        str(
            importlib.resources.files("themeda_preproc.resources.relabel").joinpath(
                "LandUseCollapse_allVersions.csv"
            )
        )
    )

    relabel_lut = np.ones(663 + 1, dtype=np.uint8) * SENTINEL_VAL

    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(f=handle)
        for row in reader:
            orig_label = themeda_preproc.utils.num_str_to_int(num_str=row["Value"])
            new_label = themeda_preproc.utils.num_str_to_int(num_str=row["NewValue"])

            if (orig_label < 0 or new_label < 0) or (
                orig_label > 663 or new_label > 12
            ):
                raise ValueError(f"Unexpected labels: {orig_label}, {new_label}")

            relabel_lut[orig_label] = new_label

    return relabel_lut


def relabel_chip(
    chip: xr.DataArray,
    relabel_lut: npt.NDArray[np.uint8],
    inplace: bool = True,
) -> xr.DataArray:
    if not inplace:
        chip = chip.copy()

    # need to manually replace the 'nodata' value with zero
    converted_chip = xr.where(
        cond=chip == chip.rio.nodata,
        x=0,
        y=chip,
    )

    converted_chip.data = relabel_lut[converted_chip.data]

    if (converted_chip == SENTINEL_VAL).any():
        raise ValueError("Unexpected relabelling; sentinel value observed")

    converted_chip.rio.set_crs(input_crs=chip.rio.crs, inplace=True)
    converted_chip.rio.set_nodata(input_nodata=0, inplace=True)

    return converted_chip


def get_lu_code_lut(attr_table_path: pathlib.Path) -> npt.NDArray[np.uint16]:
    """
    Converter from the 'Value' field, stored in the GeoTIFF raster images, to the
    land use code, stored in an attribute table that is exported from ArcCatalog.
    """

    # should be much larger than required, but to be safe
    lut = np.ones(5000, dtype=np.uint16) * SENTINEL_VAL

    with attr_table_path.open(newline="", encoding="ascii") as handle:
        reader = csv.DictReader(f=handle)
        for row in reader:
            value = themeda_preproc.utils.num_str_to_int(num_str=row["VALUE"])
            lu_code = themeda_preproc.utils.num_str_to_int(num_str=row["LU_CODE"])

            if value < 0 or lu_code < 0:
                raise ValueError(f"Unexpected labels: {value}, {lu_code}")

            lut[value] = lu_code

    lut[0] = 0

    return lut


def get_cmap() -> themeda_preproc.vis.colourmap.ColourMap:
    # colour values from NLUM_250m_DescriptiveMetadata_20220622.pdf
    cmap = themeda_preproc.vis.colourmap.ColourMap(
        name="land_use",
        entries=[
            themeda_preproc.vis.colourmap.ColourMapEntry(
                label="No data",
                value=0,
                colour=(0, 0, 0),
            ),
            themeda_preproc.vis.colourmap.ColourMapEntry(
                label="Nature conservation",
                value=1,
                colour=(150, 102, 204),
            ),
            themeda_preproc.vis.colourmap.ColourMapEntry(
                label="Managed resource protection",
                value=2,
                colour=(201, 190, 255),
            ),
            themeda_preproc.vis.colourmap.ColourMapEntry(
                label="Other minimal use",
                value=3,
                colour=(222, 135, 221),
            ),
            themeda_preproc.vis.colourmap.ColourMapEntry(
                label="Grazing native vegetation",
                value=4,
                colour=(217, 214, 207),  # (255, 255, 229) is too bright
            ),
            themeda_preproc.vis.colourmap.ColourMapEntry(
                label="Production native forests",
                value=5,
                colour=(41, 137, 68),
            ),
            themeda_preproc.vis.colourmap.ColourMapEntry(
                label="Grazing modified pastures",
                value=6,
                colour=(255, 211, 127),
            ),
            themeda_preproc.vis.colourmap.ColourMapEntry(
                label="Plantation forests",
                value=7,
                colour=(173, 255, 181),
            ),
            themeda_preproc.vis.colourmap.ColourMapEntry(
                label="Cropping",
                value=8,
                colour=(255, 255, 0),
            ),
            themeda_preproc.vis.colourmap.ColourMapEntry(
                label="Horticulture",
                value=9,
                colour=(171, 135, 120),
            ),
            themeda_preproc.vis.colourmap.ColourMapEntry(
                label="Intensive uses",
                value=10,
                colour=(255, 0, 0),
            ),
            themeda_preproc.vis.colourmap.ColourMapEntry(
                label="Mining and waste",
                value=11,
                colour=(71, 130, 143),
            ),
            themeda_preproc.vis.colourmap.ColourMapEntry(
                label="Water",
                value=12,
                colour=(0, 0, 255),
            ),
        ],
    )

    return cmap
