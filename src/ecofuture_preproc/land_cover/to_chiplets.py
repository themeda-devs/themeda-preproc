import pathlib
import functools

import ecofuture_preproc.source
import ecofuture_preproc.roi
import ecofuture_preproc.chiplet_table
import ecofuture_preproc.chiplets
import ecofuture_preproc.land_cover.labels


def run(
    source_name: ecofuture_preproc.source.DataSourceName,
    roi_name: ecofuture_preproc.roi.ROIName,
    base_output_dir: pathlib.Path,
    pad_size_pix: int,
    cores: int,
    base_size_pix: int = 160,
    protect: bool = True,
    show_progress: bool = True,
) -> None:

    table = ecofuture_preproc.chiplet_table.load_table(
        base_output_dir=base_output_dir,
        roi_name=roi_name,
        pad_size_pix=pad_size_pix,
    )

    # TMMMP
    table = table[:5]

    roi = ecofuture_preproc.roi.RegionOfInterest(
        name=roi_name,
        base_output_dir=base_output_dir,
        load=True,
    )

    coastal_roi = ecofuture_preproc.roi.RegionOfInterest(
        name=ecofuture_preproc.roi.ROIName("australia"),
        base_output_dir=base_output_dir,
        load=True,
    )

    relabel_lut = ecofuture_preproc.land_cover.labels.get_relabel_lut()

    relabeller = functools.partial(
        ecofuture_preproc.land_cover.labels.relabel_chiplet,
        relabel_lut=relabel_lut,
        coastal_roi=coastal_roi,
    )

    ecofuture_preproc.chiplets.form_chiplets(
        table=table,
        source_name=source_name,
        roi=roi,
        base_size_pix=base_size_pix,
        pad_size_pix=pad_size_pix,
        base_output_dir=base_output_dir,
        protect=protect,
        cores=cores,
        show_progress=show_progress,
        relabeller=relabeller,
    )
