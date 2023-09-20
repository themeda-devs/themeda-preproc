import csv
import pathlib
import importlib.resources
import typing

import numpy as np

import xarray as xr

import veusz.embed

import matplotlib

import ecofuture_preproc.roi
import ecofuture_preproc.source
import ecofuture_preproc.vis.utils
import ecofuture_preproc.vis.maps


def run(
    source_name: ecofuture_preproc.source.DataSourceName,
    roi_name: ecofuture_preproc.roi.ROIName,
    base_output_dir: pathlib.Path,
    protect: bool = True,
    resolution: typing.Optional[typing.Union[float, int]] = 1_000,
    headless: bool = True,
) -> None:
    ecofuture_preproc.vis.maps.plot_years(
        source_name=source_name,
        roi_name=roi_name,
        base_output_dir=base_output_dir,
        customiser=customiser,
        protect=protect,
        resolution=resolution,
        headless=headless,
    )


def customiser(
    embed: veusz.embed.Embedded,
    page: veusz.embed.WidgetNode,
    packet: xr.DataArray,  # noqa
    source_name: ecofuture_preproc.source.DataSourceName,
    year: int,
) -> None:

    (img_widget, *_) = page.WalkWidgets(widgettype="image")

    name_prefix = img_widget.name.removesuffix("_img")

    cmap_name = f"{name_prefix}_cmap"

    (cmap, max_val) = get_colour_map(
        packet=packet,
        source_name=source_name,
        year=year,
    )

    embed.AddCustom(
        ctype="colormap",
        name=cmap_name,
        val=cmap.as_veusz_colourmap(stepped=True),
    )

    img_widget.colorMap.val = cmap_name
    img_widget.min.val = 0.0
    img_widget.max.val = 255


def get_colour_map(
    packet: xr.DataArray,
    source_name: ecofuture_preproc.source.DataSourceName,
    year: int,
) -> (ecofuture_preproc.vis.utils.ColourMap, int):

    # indicate stepped
    packet_values = packet.values.flatten()
    max_val = int(np.max(packet_values[packet_values < 255]))

    grey_cmap = matplotlib.colormaps["inferno"]
    grey_cmap = grey_cmap.resampled(lutsize=max_val + 1)

    entries = [
        ecofuture_preproc.vis.utils.ColourMapEntry(
            label=str(value),
            value=value,
            colour=tuple(int(val * 255) for val in grey_cmap(value)),
        )
        for value in range(max_val + 1)
    ]

    cmap = ecofuture_preproc.vis.utils.ColourMap(
        name=f"{source_name.value}_{year}",
        entries=entries,
    )

    return (cmap, max_val)
