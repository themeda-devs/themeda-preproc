import pathlib
import typing
import functools

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
    customiser = functools.partial(
        ecofuture_preproc.vis.maps.generic_continuous_customiser,
        cbar_label="Elevation (m)",
    )

    ecofuture_preproc.vis.maps.plot_years(
        source_name=source_name,
        roi_name=roi_name,
        base_output_dir=base_output_dir,
        customiser=customiser,
        protect=protect,
        resolution=resolution,
        headless=headless,
    )
