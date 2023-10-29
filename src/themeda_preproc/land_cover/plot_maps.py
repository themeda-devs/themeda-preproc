import pathlib
import typing

import numpy as np

import xarray as xr

import veusz.embed

import themeda_preproc.roi
import themeda_preproc.source
import themeda_preproc.vis.utils
import themeda_preproc.vis.maps
import themeda_preproc.land_cover.colourmap


def run(
    source_name: themeda_preproc.source.DataSourceName,
    roi_name: themeda_preproc.roi.ROIName,
    base_output_dir: pathlib.Path,
    protect: bool = True,
    resolution: typing.Optional[typing.Union[float, int]] = 1_000,
    headless: bool = True,
) -> None:
    themeda_preproc.vis.maps.plot_years(
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
    source_name: themeda_preproc.source.DataSourceName,  # noqa
    year: int,  # noqa
) -> None:
    (img_widget, *_) = page.WalkWidgets(widgettype="image")

    name_prefix = img_widget.name.removesuffix("_img")

    cmap_name = f"{name_prefix}_cmap"

    cmap = get_colour_map()

    embed.AddCustom(
        ctype="colormap",
        name=cmap_name,
        val=cmap.as_veusz_colourmap(),
    )

    img_widget.colorMap.val = cmap_name

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

    embed.SetDataText(
        name=f"{name_prefix}_cbar_labels",
        val=[entry.label for entry in entries],
    )

    dummy_xy = cbar_graph.Add("xy")
    dummy_xy.xData.val = [0] * n_entries
    dummy_xy.yData.val = np.arange(n_entries).tolist()
    dummy_xy.labels.val = f"{name_prefix}_cbar_labels"

    themeda_preproc.vis.utils.set_margins(
        widget=cbar_graph,
        margins={
            "L": "11.37cm",
            "R": "3.243cm",
            "T": "1.059cm",
            "B": "0.577cm",
        },
    )

    themeda_preproc.vis.utils.set_margins(
        widget=graph,
        margins={"R": "3.929cm"},
        null_absent=True,
    )


# for backward compatability
get_colour_map = themeda_preproc.land_cover.colourmap.get_colour_map
