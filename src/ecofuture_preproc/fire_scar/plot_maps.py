import pathlib
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
    packet: xr.DataArray,
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

    entries = sorted(cmap.entries, key=lambda x: x.value)

    n_entries = len(entries)

    dummy_colourbar_img = np.arange(n_entries, dtype=np.uint8)[:, np.newaxis]

    embed.SetData2D(
        name=f"{name_prefix}_cbar_data",
        data=dummy_colourbar_img,
        xcent=[0],
        ycent=np.arange(n_entries),
    )

    cbar_graph = page.Add("graph", autoadd=False)

    (base_graph, *_) = page.WalkWidgets(widgettype="graph")
    graph = base_graph.Clone(newparent=page)
    base_graph.Remove()

    cbar_x = cbar_graph.Add("axis")
    cbar_y = cbar_graph.Add("axis")

    cbar_img = cbar_graph.Add("image")
    cbar_img.data.val = f"{name_prefix}_cbar_data"
    cbar_img.colorMap.val = cmap_name
    cbar_img.min.val = 0
    cbar_img.max.val = 255

    (cbar_x.min.val, cbar_x.max.val) = (-0.5, +0.5)
    (cbar_y.min.val, cbar_y.max.val) = (-0.5, n_entries - 0.5)

    cbar_y.mode.val = "labels"
    cbar_y.otherPosition.val = 1
    cbar_y.TickLabels.offset.val = "4pt"
    cbar_y.lowerPosition.val = cbar_x.lowerPosition.val = 0.0
    cbar_y.upperPosition.val = cbar_y.upperPosition.val = 1.0
    cbar_x.hide.val = True
    cbar_y.MinorTicks.hide.val = True
    cbar_y.MajorTicks.manualTicks.val = np.arange(n_entries).tolist()
    cbar_y.Line.hide.val = True
    cbar_y.label.val = "Count"
    cbar_y.Label.rotate.val = "180"

    embed.SetDataText(
        name=f"{name_prefix}_cbar_labels",
        val=[entry.label for entry in entries],
    )

    dummy_xy = cbar_graph.Add("xy")
    dummy_xy.xData.val = [0] * n_entries
    dummy_xy.yData.val = np.arange(n_entries).tolist()
    dummy_xy.labels.val = f"{name_prefix}_cbar_labels"

    ecofuture_preproc.vis.utils.set_margins(
        widget=cbar_graph,
        margins={
            "L": "12.111cm",
            "R": "2.502cm",
            "T": "3.493cm",
            "B": "3.144cm",
        },
    )

    ecofuture_preproc.vis.utils.set_margins(
        widget=graph,
        margins={"R": "3.929cm"},
        null_absent=True,
    )


def get_colour_map(
    packet: xr.DataArray,
    source_name: ecofuture_preproc.source.DataSourceName,
    year: int,
) -> tuple[ecofuture_preproc.vis.utils.ColourMap, int]:

    # indicate stepped
    packet_values = packet.values.flatten()
    max_val = int(np.max(packet_values[packet_values < 255]))

    if max_val > 2:
        raise ValueError("Unexpected maximum")

    max_val = 2

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
