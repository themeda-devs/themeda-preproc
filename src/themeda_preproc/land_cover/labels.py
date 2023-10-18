import csv
import pathlib
import importlib.resources
import typing

import numpy as np
import numpy.typing as npt

import xarray as xr

import themeda_preproc.roi
import themeda_preproc.utils


SENTINEL_VAL: typing.Final = 99

WATER_LABELS: typing.Final = (20, 21)

OCEAN_LABEL: typing.Final = 22


def relabel_chiplet(
    chiplet: xr.DataArray,
    partial_roi_overlap: bool,
    relabel_lut: npt.NDArray[np.uint8],
    coastal_roi: themeda_preproc.roi.RegionOfInterest,
    inplace: bool = True,
) -> xr.DataArray:
    if not inplace:
        chiplet = chiplet.copy()

    chiplet.data = relabel_lut[chiplet.data]

    # if some of the pixels are not inside the ROI, we need to potentially do
    # some additional re-labelling
    if partial_roi_overlap:
        # relabelling is only on water classifications, so we can skip if there
        # aren't any water pixels
        is_water_pixel = np.any(
            [chiplet == water_label for water_label in WATER_LABELS],
            axis=0,
        )

        if np.any(is_water_pixel):
            # OK, so we have pixels that are water
            # we need to work out whether they are in the ocean or are inland

            # if they are ocean, then they will be outside the ROI

            # rasterise the coastal ROI and convert to a boolean mask
            is_outside_roi = (
                chiplet.rio.set_nodata(input_nodata=255, inplace=False).rio.clip(
                    geometries=[coastal_roi.shape], drop=False
                )
            ) == 255

            # we care if it is both outside the ROI and is classified as water
            outside_roi_and_is_water = np.logical_and(
                is_outside_roi,
                is_water_pixel,
            )

            # if both of those things are true, then assign it as ocean
            chiplet = xr.where(
                cond=outside_roi_and_is_water,
                x=OCEAN_LABEL,
                y=chiplet,
            )

    if (chiplet == SENTINEL_VAL).any():
        raise ValueError("Unexpected relabelling; sentinel value observed")

    return chiplet


def get_relabel_lut() -> npt.NDArray[np.uint8]:
    csv_path = pathlib.Path(
        str(
            importlib.resources.files("themeda_preproc.resources.relabel").joinpath(
                "DEALC_to_LCNS_v2.csv"
            )
        )
    )

    relabel_lut = np.ones(128, dtype=np.uint8) * SENTINEL_VAL

    with csv_path.open(newline="", encoding="latin") as handle:
        reader = csv.DictReader(f=handle)
        for row in reader:
            dea_level_4 = themeda_preproc.utils.num_str_to_int(num_str=row["level4"])
            new_label = themeda_preproc.utils.num_str_to_int(num_str=row["LCNS_n"])

            if (dea_level_4 < 0 or new_label < 0) or (
                dea_level_4 > 127 or new_label > 255
            ):
                raise ValueError(f"Unexpected labels: {dea_level_4}, {new_label}")

            relabel_lut[dea_level_4] = new_label

    return relabel_lut
