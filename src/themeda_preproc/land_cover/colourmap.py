import csv
import pathlib
import importlib.resources

import themeda_preproc.vis.colourmap


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
