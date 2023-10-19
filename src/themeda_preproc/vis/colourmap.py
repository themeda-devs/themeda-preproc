import typing
import typing_extensions
import dataclasses

RGBType: typing_extensions.TypeAlias = tuple[int, int, int]
RGBAType: typing_extensions.TypeAlias = tuple[int, int, int, int]


@dataclasses.dataclass
class ColourMapEntry:
    label: str
    value: int
    colour: typing.Union[RGBType, RGBAType]

    @property
    def hex(self) -> str:  # noqa: A003
        return f"#{self.colour[0]:02x}{self.colour[1]:02x}{self.colour[2]:02x}"


@dataclasses.dataclass
class ColourMap:
    name: str
    entries: list[ColourMapEntry]

    def __len__(self) -> int:
        return len(self.entries)

    def __iter__(self) -> typing.Iterator[ColourMapEntry]:
        return iter(self.entries)

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
