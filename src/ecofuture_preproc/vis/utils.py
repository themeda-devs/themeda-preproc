
import pathlib
import typing
import platform
import importlib.resources
import ast

import distro

import veusz.embed


def set_veusz_style(
    embed: veusz.embed.Embedded,
    font: typing.Optional[typing.Union[str, dict[str, str]]] =  None,
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
                ast.literal_eval(arg.strip())
                for arg in args.split(",", maxsplit=1)
            ]

            method = getattr(embed, command)

            method(*args)
