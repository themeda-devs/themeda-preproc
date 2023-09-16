

import pathlib
import typing

import numpy as np

import xarray as xr

import veusz.embed

import ecofuture_preproc.source
import ecofuture_preproc.roi
import ecofuture_preproc.vis.utils


def run(
    source_name: ecofuture_preproc.source.DataSourceName,
    roi_name: ecofuture_preproc.roi.ROIName,
    base_output_dir: pathlib.Path,
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


def render_year(
    embed: veusz.embed.Embedded,
    year: int,
    source_name: ecofuture_preproc.source.DataSourceName,
    chiplets_path: pathlib.Path,
    resolution: typing.Union[float, int],
):

    page = embed.Root.Add("page")
    page.width.val = "15cm"
    page.height.val = "15cm"


def get_packet(
    year: int,
    source_name: ecofuture_preproc.source.DataSourceName,
    chiplets_path: pathlib.Path,
    resolution: typing.Union[float, int],
) -> xr.DataArray:

    paths = sorted((chiplets_path / str(year)).glob("*.tif"))

    resampling = ecofuture_preproc.source.DATA_SOURCE_RESAMPLERS[source_name]

    packet = ecofuture_preproc.packet.form_packet(
        paths=paths,
        new_resolution=resolution,
        nodata=np.nan,
        resampling=resampling,
    )

    return packet
