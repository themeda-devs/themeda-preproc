import pathlib

import themeda_preproc.source
import themeda_preproc.roi
import themeda_preproc.chiplet_table
import themeda_preproc.chiplets


def run(
    source_name: themeda_preproc.source.DataSourceName,
    roi_name: themeda_preproc.roi.ROIName,
    base_output_dir: pathlib.Path,
    pad_size_pix: int,
    cores: int,
    base_size_pix: int = 160,
    protect: bool = True,
    show_progress: bool = True,
) -> None:
    table = themeda_preproc.chiplet_table.load_table(
        base_output_dir=base_output_dir,
        roi_name=roi_name,
        pad_size_pix=pad_size_pix,
    )

    roi = themeda_preproc.roi.RegionOfInterest(
        name=roi_name,
        base_output_dir=base_output_dir,
        load=True,
    )

    themeda_preproc.chiplets.form_chiplets(
        table=table,
        source_name=source_name,
        roi=roi,
        base_size_pix=base_size_pix,
        pad_size_pix=pad_size_pix,
        base_output_dir=base_output_dir,
        protect=protect,
        cores=cores,
        show_progress=show_progress,
        load_chips_masked=True,
        form_packet_via_rioxarray=True,
    )
