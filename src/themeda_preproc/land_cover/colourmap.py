import csv
import pathlib
import importlib.resources

import numpy as np

import skimage.color

import themeda_preproc.vis.colourmap
import themeda_preproc.utils


def get_colour_map() -> themeda_preproc.vis.colourmap.ColourMap:
    csv_path = pathlib.Path(
        str(
            importlib.resources.files("themeda_preproc.resources.relabel").joinpath(
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

            value = themeda_preproc.utils.num_str_to_int(num_str=row["LCNS_n"])
            label = row["LCNS_label"]
            colour = themeda_preproc.vis.colourmap.hex_to_rgb(colour=row["LCNS_HexCol"])
            entries.append(
                themeda_preproc.vis.colourmap.ColourMapEntry(
                    label=label,
                    value=value,
                    colour=colour,
                )
            )

    cmap = themeda_preproc.vis.colourmap.ColourMap(
        name="land_cover",
        entries=entries,
    )

    return cmap


def get_colour_map_level0() -> themeda_preproc.vis.colourmap.ColourMap:
    csv_path = pathlib.Path(
        str(
            importlib.resources.files("themeda_preproc.resources.relabel").joinpath(
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

            value = themeda_preproc.utils.num_str_to_int(num_str=row["LCNS_n"])
            label = row["LCNS_lev0_label"]
            colour = themeda_preproc.vis.colourmap.hex_to_rgb(colour=row["LCNS_HexCol"])
            entries.append(
                themeda_preproc.vis.colourmap.ColourMapEntry(
                    label=label,
                    value=value,
                    colour=colour,
                )
            )

    labels = {entry.label for entry in entries}

    revised_colours = {}

    for label in labels:
        label_colours = [entry.colour for entry in entries if entry.label == label]

        if len(label_colours) > 1:
            # colours as [0,1] rgb
            rgb = np.array(label_colours)[:, :-1] / 255.0

            # average in lab space
            lab = np.mean(skimage.color.rgb2lab(rgb=rgb), axis=0)

            # convert back to rgb
            rgb_level0 = np.around(skimage.color.lab2rgb(lab=lab) * 255).astype(
                np.uint8
            )

            label_colour = (*tuple(rgb_level0), 255)

        else:
            (label_colour,) = label_colours

        revised_colours[label] = label_colour

    for entry in entries:
        entry.colour = revised_colours[entry.label]

    cmap = themeda_preproc.vis.colourmap.ColourMap(
        name="land_cover_L0",
        entries=entries,
    )

    return cmap
