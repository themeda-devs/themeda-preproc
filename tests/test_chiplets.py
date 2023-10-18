import types

import numpy as np

import xarray as xr

import themeda_preproc.chiplets
import themeda_preproc.source
import themeda_preproc.roi


# demo chiplet metadata
METADATA = types.MappingProxyType(
    {
        "partial_roi_overlap": True,
        "chip_i_x_base": 480,
        "chip_i_y_base": 3840,
        "chip_grid_ref_x_base": -10,
        "chip_grid_ref_y_base": -18,
        "bbox_left": -988000,
        "bbox_bottom": -1800000,
        "bbox_right": -984000,
        "bbox_top": -1796000,
        "pad_bbox_left": -988800,
        "pad_bbox_bottom": -1800800,
        "pad_bbox_right": -983200,
        "pad_bbox_top": -1795200,
        "chip_transform_i_to_coords_coeff_a": 25.0,
        "chip_transform_i_to_coords_coeff_b": 0.0,
        "chip_transform_i_to_coords_coeff_c": -1000000.0,
        "chip_transform_i_to_coords_coeff_d": 0.0,
        "chip_transform_i_to_coords_coeff_e": -25.0,
        "chip_transform_i_to_coords_coeff_f": -1700000.0,
        "chip_transform_i_to_coords_coeff_g": 0.0,
        "chip_transform_i_to_coords_coeff_h": 0.0,
        "chip_transform_i_to_coords_coeff_i": 1.0,
        "index": 0,
        "subset_num": 4,
        "subset_instance_num": 1,
    }
)


def test_parse_chiplet_filename(base_output_dir):
    for source_name in themeda_preproc.source.DataSourceName:
        for year in [1988, 2001]:
            for roi_name in themeda_preproc.roi.ROIName:
                for pad_size_pix in [0, 32]:
                    path = themeda_preproc.chiplets.get_chiplet_path(
                        source_name=source_name,
                        year=year,
                        roi_name=roi_name,
                        pad_size_pix=pad_size_pix,
                        base_output_dir=base_output_dir,
                    )

                    true_info = themeda_preproc.chiplets.ChipletFilenameInfo(
                        path=path,
                        roi_name=roi_name,
                        source_name=source_name,
                        year=year,
                        pad_size_pix=pad_size_pix,
                    )

                    inferred_info = themeda_preproc.chiplets.parse_chiplet_filename(
                        filename=path
                    )

                    assert true_info == inferred_info


def test_get_chiplet_from_packet():
    transform = themeda_preproc.chiplets.get_transform_from_row(row=METADATA)

    true_chiplet = convert_chiplet_to_data_array(pad_size_pix=0)

    inferred_chiplet = themeda_preproc.chiplets.get_chiplet_from_packet(
        packet=true_chiplet,
        chip_i_x_base=METADATA["chip_i_x_base"],
        chip_i_y_base=METADATA["chip_i_y_base"],
        chip_i_to_coords_transform=transform,
        base_size_pix=160,
        pad_size_pix=0,
    )

    assert inferred_chiplet.equals(true_chiplet)


def test_convert_chiplet_to_data_array():
    for pad_size_pix in [0, 32]:
        convert_chiplet_to_data_array(pad_size_pix=pad_size_pix)


def convert_chiplet_to_data_array(pad_size_pix):
    base_size_pix = 160

    base_array = np.random.rand(base_size_pix, base_size_pix)

    raw_array = np.pad(
        array=base_array,
        pad_width=pad_size_pix,
        constant_values=999.0,
    )

    da = themeda_preproc.chiplets.convert_chiplet_to_data_array(
        chiplet=raw_array,
        metadata=METADATA,
        pad_size_pix=pad_size_pix,
        base_size_pix=base_size_pix,
    )

    assert np.allclose(da.values, base_array)

    pixel_delta = 25

    assert da.x.min() == (METADATA["bbox_left"] + pixel_delta / 2)
    assert da.x.max() == (METADATA["bbox_right"] - pixel_delta / 2)
    assert da.y.min() == (METADATA["bbox_bottom"] + pixel_delta / 2)
    assert da.y.max() == (METADATA["bbox_top"] - pixel_delta / 2)

    return da
