import pathlib
import typing

import xarray as xr

import veusz.embed

import themeda_preproc.source
import themeda_preproc.roi
import themeda_preproc.packet
import themeda_preproc.vis.utils


def plot_years(
    source_name: themeda_preproc.source.DataSourceName,
    roi_name: themeda_preproc.roi.ROIName,
    base_output_dir: pathlib.Path,
    customiser: typing.Callable[
        [
            veusz.embed.Embedded,
            veusz.embed.WidgetNode,
            xr.DataArray,
            themeda_preproc.source.DataSourceName,
            int,
        ],
        None,
    ],
    protect: bool = True,
    resolution: typing.Union[float, int] = 1_000,
    headless: bool = True,
    set_minmax_across_years: bool = False,
) -> None:
    chiplets_path = (
        base_output_dir
        / "chiplets_geotiff"
        / f"roi_{roi_name.value}"
        / source_name.value
    )

    output_path = (
        base_output_dir
        / "plot_maps"
        / f"roi_{roi_name.value}"
        / source_name.value
        / f"{source_name.value}_maps.pdf"
    )

    if themeda_preproc.utils.is_path_existing_and_read_only(
        path=output_path,
    ):
        return

    output_path.parent.mkdir(exist_ok=True, parents=True)

    years = themeda_preproc.utils.get_years_in_path(path=chiplets_path)

    embed = veusz.embed.Embedded(hidden=headless)
    themeda_preproc.vis.utils.set_veusz_style(embed=embed)

    if set_minmax_across_years:
        min_val = None
        max_val = None

    for year in years:
        packet = render_year(
            embed=embed,
            year=year,
            source_name=source_name,
            chiplets_path=chiplets_path,
            resolution=resolution,
            customiser=customiser,
        )

        if set_minmax_across_years:
            packet_min = packet.min().item()
            packet_max = packet.max().item()

            if min_val is None:
                min_val = packet_min
            if max_val is None:
                max_val = packet_max

            min_val = min(min_val, packet_min)
            max_val = max(max_val, packet_max)

        packet.close()

        del packet

    if set_minmax_across_years:
        assert min_val is not None
        assert max_val is not None

        set_img_minmax_vals(embed=embed, min_val=min_val, max_val=max_val)

    embed.WaitForClose()

    embed.Export(
        str(output_path),
        page=themeda_preproc.vis.utils.get_page_list(embed=embed),
    )

    if protect:
        themeda_preproc.utils.protect_path(path=output_path)


def render_year(
    embed: veusz.embed.Embedded,
    year: int,
    source_name: themeda_preproc.source.DataSourceName,
    chiplets_path: pathlib.Path,
    resolution: typing.Union[float, int],
    customiser: typing.Callable[
        [
            veusz.embed.Embedded,
            veusz.embed.WidgetNode,
            xr.DataArray,
            themeda_preproc.source.DataSourceName,
            int,
        ],
        None,
    ],
    packet: typing.Optional[xr.DataArray] = None,
) -> xr.DataArray:
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

    themeda_preproc.vis.utils.set_margins(
        widget=graph,
        margins={"R": "3.929cm"},
        null_absent=True,
    )

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

    x_axis.MinorTicks.hide.val = y_axis.MinorTicks.hide.val = True
    x_axis.TickLabels.format.val = y_axis.TickLabels.format.val = "%d"

    x_axis.lowerPosition.val = y_axis.lowerPosition.val = 0.0
    x_axis.upperPosition.val = y_axis.upperPosition.val = 1.0

    x_axis.max.val = 1.75e6

    x_axis.hide.val = y_axis.hide.val = True

    if customiser is not None:
        customiser(embed, page, packet, source_name, year)

    return packet


def set_img_minmax_vals(
    embed: veusz.embed.Embedded,
    min_val: typing.Union[float, int],
    max_val: typing.Union[float, int],
) -> None:
    for img_widget in embed.Root.WalkWidgets(widgettype="image"):
        img_widget.min.val = min_val
        img_widget.max.val = max_val


def generic_continuous_customiser(
    embed: veusz.embed.Embedded,  # noqa
    page: veusz.embed.WidgetNode,
    packet: xr.DataArray,  # noqa
    source_name: themeda_preproc.source.DataSourceName,  # noqa
    year: int,  # noqa
    cmap_name: str = "viridis",
    cbar_label: str = "",
) -> None:
    (img_widget, *_) = page.WalkWidgets(widgettype="image")

    img_widget.colorMap.val = cmap_name

    (graph, *_) = page.WalkWidgets(widgettype="graph")

    cbar = graph.Add("colorbar")

    cbar.image.val = img_widget.name
    cbar.otherPosition.val = 1
    cbar.TickLabels.offset.val = "4pt"
    cbar.Border.hide.val = True
    cbar.direction.val = "vertical"
    cbar.horzManual.val = 1.1
    cbar.Label.rotate.val = "180"
    cbar.MinorTicks.hide.val = True
    cbar.label.val = cbar_label
    cbar.TickLabels.size.val = "6pt"
    cbar.MajorTicks.length.val = "3pt"
    cbar.MajorTicks.width.val = "0.5pt"
    cbar.outerticks.val = True
    cbar.horzPosn.val = "manual"


def get_packet(
    year: int,
    source_name: themeda_preproc.source.DataSourceName,
    chiplets_path: pathlib.Path,
    resolution: typing.Optional[typing.Union[float, int]],
    form_via_rioxarray: bool = True,
) -> xr.DataArray:
    paths = sorted((chiplets_path / str(year)).glob("*.tif"))

    nodata = themeda_preproc.source.DATA_SOURCE_SENTINEL[source_name]

    resampling = themeda_preproc.source.DATA_SOURCE_RESAMPLERS[source_name]

    packet = themeda_preproc.packet.form_packet(
        paths=paths,
        form_via_rioxarray=form_via_rioxarray,
        new_resolution=resolution,
        nodata=nodata,
        resampling=resampling,
    )

    return packet
