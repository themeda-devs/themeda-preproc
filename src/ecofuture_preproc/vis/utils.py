import pathlib
import typing
import platform
import importlib.resources
import ast
import dataclasses

import distro
import typing_extensions

import veusz.embed


RGBType: typing_extensions.TypeAlias = tuple[int, int, int]
RGBAType: typing_extensions.TypeAlias = tuple[int, int, int, int]


@dataclasses.dataclass
class ColourMapEntry:
    label: str
    value: int
    colour: typing.Union[RGBType, RGBAType]


@dataclasses.dataclass
class ColourMap:
    name: str
    entries: list[ColourMapEntry]

    def as_veusz_colourmap(self, stepped: bool = False) -> list[RGBAType]:
        cmap = [(0, 0, 0, 0) for _ in range(256)]

        for entry in self.entries:
            entry_colour = entry.colour

            if len(entry_colour) == 3:
                entry_colour = (*typing.cast(RGBType, entry_colour), 255)

            entry_colour = typing.cast(RGBAType, entry_colour)

            cmap[entry.value] = entry_colour

        if stepped:
            cmap.insert(0, (-1, 0, 0, 0))

        return cmap


def hex_to_rgb(colour: str) -> RGBAType:
    if not colour.startswith("#"):
        raise ValueError("Expecting the hex code to start with #")

    hex_chars = list(colour[1:])

    rgba_vals = tuple(
        int("".join(hex_pair), 16) for hex_pair in zip(hex_chars[::2], hex_chars[1::2])
    )

    if len(rgba_vals) == 3:
        rgba_vals += (255,)

    rgba = typing.cast(RGBAType, rgba_vals)

    return rgba


def set_veusz_style(
    embed: veusz.embed.Embedded,
    font: typing.Optional[typing.Union[str, dict[str, str]]] = None,
    font_size_mod: typing.Union[int, float] = 0,
) -> None:
    style_path = pathlib.Path(
        str(
            importlib.resources.files("ecofuture_preproc.resources.vis").joinpath(
                "veusz_stylesheet.vst"
            )
        )
    )

    if font is None:
        font = {
            "ubuntu": "Nimbus Sans L",
            "arch": "Nimbus Sans [UKWN]",
            "windows": "Arial",
        }

    if isinstance(font, str):
        font_family = font

    elif isinstance(font, dict):
        font_family = None

        if "windows" in font and platform.system() == "Windows":
            font_family = font["windows"]
        else:
            distro_name = distro.id()
            font_family = font[distro_name]

        if font_family is None:
            raise ValueError("Unknown font descriptor")

    assert font_family is not None

    with style_path.open("r", encoding="utf-8") as style_file:
        style_lines = style_file.readlines()

    for style_line in style_lines:
        style_line = style_line.strip()

        if font is not None:
            style_line = style_line.replace("Arial", font_family)

        if "size" in style_line:
            (param, value) = style_line.split(", ")

            if "pt" in value:
                if value.startswith("u"):
                    (pt, _) = value[2:].split("pt")
                    new_pt = str(int(pt) + font_size_mod)
                    value = r"u'" + f"{new_pt}pt')"
                style_line = ", ".join((param, value))

        if style_line.startswith("Set"):
            assert style_line.endswith(")")

            style_line = style_line[:-1]

            (command, args) = style_line.split("(", maxsplit=1)

            args = [
                ast.literal_eval(arg.strip()) for arg in args.split(",", maxsplit=1)
            ]

            method = getattr(embed, command)

            method(*args)


def set_margins(
    widget: veusz.embed.WidgetNode,
    margins: typing.Optional[dict[str, str]] = None,
    null_absent: bool = False,
) -> None:
    side_lut = {
        "L": "left",
        "R": "right",
        "T": "top",
        "B": "bottom",
        "I": "internal",
    }

    null_margins = {side: "0cm" for side in side_lut}

    if margins is None:
        margins = null_margins

    if null_absent:
        margins = {**null_margins, **margins}

    for side, margin in margins.items():
        try:
            widget_margin = getattr(widget, f"{side_lut[side]:s}Margin")
        except AttributeError:
            assert side == "I"
            continue

        widget_margin.val = margin


def get_n_pages(embed: veusz.embed.Embedded) -> int:
    """Returns the number of pages in a veusz figure."""

    n_pages = len(list(embed.Root.WalkWidgets(widgettype="page")))

    return n_pages


def get_page_list(embed: veusz.embed.Embedded) -> list[int]:
    """Returns a list with the pages in a veusz figure. Useful for passing as
    the `page` argument to `embed.Export`."""

    n_pages = get_n_pages(embed=embed)

    return list(range(n_pages))
