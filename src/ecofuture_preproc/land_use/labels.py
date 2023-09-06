import csv
import pathlib
import importlib.resources
import typing

import numpy as np
import numpy.typing as npt

import xarray as xr

import ecofuture_preproc.roi
import ecofuture_preproc.utils


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
            importlib.resources.files("ecofuture_preproc.resources.relabel").joinpath(
                "LandUseCollapse_allVersions.csv"
            )
        )
    )

    relabel_lut = np.ones(663 + 1, dtype=np.uint8) * SENTINEL_VAL

    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(f=handle)
        for row in reader:

            orig_label = ecofuture_preproc.utils.num_str_to_int(num_str=row["Value"])
            new_label = ecofuture_preproc.utils.num_str_to_int(num_str=row["NewValue"])

            if (
                (orig_label < 0 or new_label < 0)
                or (orig_label > 663 or new_label > 12)
            ):
                raise ValueError(f"Unexpected labels: {orig_label}, {new_label}")

            relabel_lut[orig_label] = new_label

    return relabel_lut
