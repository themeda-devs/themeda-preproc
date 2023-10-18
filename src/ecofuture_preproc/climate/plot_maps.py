import pathlib
import typing
import functools

import themeda_preproc.roi
import themeda_preproc.source
import themeda_preproc.vis.utils
import themeda_preproc.vis.maps


def run(
    source_name: themeda_preproc.source.DataSourceName,
    roi_name: themeda_preproc.roi.ROIName,
    base_output_dir: pathlib.Path,
    protect: bool = True,
    resolution: typing.Optional[typing.Union[float, int]] = 1_000,
    headless: bool = True,
) -> None:
    if source_name == themeda_preproc.source.DataSourceName.RAIN:
        cbar_label = "Annual rainfall (mm)"
    elif source_name == themeda_preproc.source.DataSourceName.TMAX:
        cbar_label = "Average maximum temperature (^\circ C)"
    else:
        raise ValueError("Unexpected source")

    customiser = functools.partial(
        themeda_preproc.vis.maps.generic_continuous_customiser,
        cbar_label=cbar_label,
    )

    themeda_preproc.vis.maps.plot_years(
        source_name=source_name,
        roi_name=roi_name,
        base_output_dir=base_output_dir,
        customiser=customiser,
        protect=protect,
        resolution=resolution,
        headless=headless,
        set_minmax_across_years=True,
    )
