

import pathlib
import typing

import xarray as xr

import veusz.embed

import ecofuture_preproc.source
import ecofuture_preproc.roi
import ecofuture_preproc.packet
import ecofuture_preproc.vis.utils


def plot_years(
    source_name: ecofuture_preproc.source.DataSourceName,
    roi_name: ecofuture_preproc.roi.ROIName,
    base_output_dir: pathlib.Path,
    customiser: typing.Callable[
        [veusz.embed.Embedded, veusz.embed.WidgetNode, xr.DataArray],
        None
    ],
    protect: bool = True,
    resolution: typing.Optional[typing.Union[float, int]] = 1_000,
    headless: bool = True,
) -> None:

    chiplets_path = (
        base_output_dir
        / "chiplets_geotiff"
        / f"roi_{roi_name.value}"
        / source_name.value
    )

    years = sorted(
        [
            int(path.name)
            for path in chiplets_path.glob("*")
            if path.is_dir()
        ]
    )

    embed = veusz.embed.Embedded(hidden=headless)
    ecofuture_preproc.vis.utils.set_veusz_style(embed=embed)

    embed.WaitForClose()

    return embed


def render_year(
    embed: veusz.embed.Embedded,
    year: int,
    source_name: ecofuture_preproc.source.DataSourceName,
    chiplets_path: pathlib.Path,
    resolution: typing.Union[float, int],
    customiser: typing.Callable[
        [veusz.embed.Embedded, veusz.embed.WidgetNode, xr.DataArray],
        None
    ],
    packet: typing.Optional[xr.DataArray] = None,
) -> None:

    if packet is None:
        packet = get_packet(
            year=year,
            source_name=source_name,
            chiplets_path=chiplets_path,
            resolution=resolution,
        )

    packet = packet.sortby(variables="y")

    page = embed.Root.Add("page")
    page.width.val = "15cm"
    page.height.val = "10cm"

    label = page.Add("label")

    source_name_label = source_name.value.replace("_", " ").title()
    label.label.val = f"{source_name_label} ({year})"
    label.alignVert.val = "top"
    label.xPos.val = 0.01
    label.yPos.val = 0.99

    graph = page.Add("graph", autoadd=False)

    graph.aspect.val = packet.sizes["x"] / packet.sizes["y"]

    ecofuture_preproc.vis.utils.set_margins(widget=graph, null_absent=True)

    x_axis = graph.Add("axis")
    y_axis = graph.Add("axis")

    data_name = f"{source_name.value}_{year}"

    embed.SetData2D(
        name=data_name,
        data=packet.values,
        xcent=packet.x.values,
        ycent=packet.y.values,
    )

    img_name = f"{data_name}_img"

    img = graph.Add("image", name=img_name)
    img.data.val = data_name
    #img.colorMap.val = f"{source_name.value}_cmap"

    x_axis.MinorTicks.hide.val = y_axis.MinorTicks.hide.val = True
    x_axis.TickLabels.format.val = y_axis.TickLabels.format.val = "%d"

    x_axis.lowerPosition.val = y_axis.lowerPosition.val = 0.0
    x_axis.upperPosition.val = y_axis.upperPosition.val = 1.0

    x_axis.max.val = 1.75e6

    x_axis.hide.val = y_axis.hide.val = True

    if customiser is not None:
        customiser(embed, page, packet)


def get_packet(
    year: int,
    source_name: ecofuture_preproc.source.DataSourceName,
    chiplets_path: pathlib.Path,
    resolution: typing.Union[float, int],
) -> xr.DataArray:

    paths = sorted((chiplets_path / str(year)).glob("*.tif"))

    nodata = ecofuture_preproc.source.DATA_SOURCE_SENTINEL[source_name]

    resampling = ecofuture_preproc.source.DATA_SOURCE_RESAMPLERS[source_name]

    packet = ecofuture_preproc.packet.form_packet(
        paths=paths,
        new_resolution=resolution,
        nodata=nodata,
        resampling=resampling,
    )

    return packet
