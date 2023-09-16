import csv
import pathlib
import importlib.resources
import typing
import dataclasses

import numpy as np
import numpy.typing as npt

import xarray as xr

import ecofuture_preproc.roi
import ecofuture_preproc.vis.utils



def get_colour_map() -> npt.NDArray[np.uint8]:

    csv_path = pathlib.Path(
        str(
            importlib.resources.files("ecofuture_preproc.resources.relabel").joinpath(
                "LCNS_codes_colours.csv"
            )
        )
    )

    entries = []

    with csv_path.open(newline="", encoding="latin") as handle:
        reader = csv.DictReader(f=handle)
        for row in reader:

            if all(row_val == "" for row_val in row.values()):
                continue

            value = num_str_to_int(num_str=row["LCNS_n"])
            label = row["LCNS_label"]
            colour = ecofuture_preproc.vis.utils.hex_to_rgb(
                colour=row["LCNS_HexCol"]
            )
            entries.append(
                ecofuture_preproc.vis.utils.ColourMapEntry(
                    label=label,
                    value=value,
                    colour=colour,
                )
            )

    cmap = ecofuture_preproc.vis.utils.ColourMap(
        name="land_cover",
        entries=entries,
    )

    return cmap


def num_str_to_int(num_str: str) -> int:
    num = float(num_str)
    if not num.is_integer():
        raise ValueError(f"{num_str} is not an integer")
    return int(num)
