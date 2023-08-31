
import numpy as np

import xarray as xr

import polars as pl

import affine

import ecofuture_preproc.roi


def form_chiplet_table(
    chips: list[xr.DataArray],
    roi: ecofuture_preproc.roi.RegionOfInterest,
    pad_size_pix: int,
    base_size_pix: int = 160,
) -> pl.dataframe.frame.DataFrame:

    pass


def form_chiplet_table_entry(
    chip: xr.DataArray,
    roi: ecofuture_preproc.roi.RegionOfInterest,
    base_size_pix: int,
    pad_size_pix: int,
) -> pl.dataframe.frame.DataFrame:

    chip_transform = chip.rio.transform()

    chip_grid_ref = ecofuture_preproc.chips.get_grid_ref_from_chip(chip=chip)

    schema = {
        f"{pad_prefix}bbox_{pos}": pl.Int64
        for pad_prefix in ["", "pad_"]
        for pos in ["left", "bottom", "right", "top"]
    }

    schema = {
        **schema,
        "chip_i_x_base": pl.UInt64,
        "chip_i_y_base": pl.UInt64,
        "chip_grid_ref_x_base": pl.Int64,
        "chip_grid_ref_y_base": pl.Int64,
    }

    schema = {
        **schema,
        **{
            f"chip_transform_i_to_coords_coeff_{letter}": pl.Float64
            for letter in "abcdefghi"
        },
    }

    common_columns = {
        "chip_grid_ref_x_base": chip_grid_ref.x,
        "chip_grid_ref_y_base": chip_grid_ref.y,
        **{
            f"chip_transform_i_to_coords_coeff_{letter}": getattr(
                chip_transform,
                letter,
            )
            for letter in "abcdefghi"
        },
    }

    rows = []

    for chip_i_x_base in range(0, chip.sizes["x"], base_size_pix):
        for chip_i_y_base in range(0, chip.sizes["y"], base_size_pix):

            (base_bbox, padded_bbox) = (
                get_bbox(
                    i_x_base=chip_i_x_base,
                    i_y_base=chip_i_y_base,
                    base_size_pix=base_size_pix,
                    pad_size_pix=bbox_pad_size_pix,
                    transform=chip_transform,
                )
                for bbox_pad_size_pix in [0, pad_size_pix]
            )

            row_data = {
                **common_columns,
                "chip_i_x_base": chip_i_x_base,
                "chip_i_y_base": chip_i_y_base,
                **{
                    f"{pad_prefix}bbox_{pos}": bbox[pos]
                    for (bbox, pad_prefix) in zip((base_bbox, padded_bbox), ["", "pad_"])
                    for pos in ["left", "bottom", "right", "top"]
                }
            }

            rows.append(row_data)

    data = pl.DataFrame(data=rows, schema=schema)

    return data


def get_bbox(
    i_x_base: int,
    i_y_base: int,
    base_size_pix: int,
    pad_size_pix: int,
    transform: affine.Affine,
) -> dict[str, float]:

    (x, y) = (
        transform
        * np.array(
            [
                [
                    i_x_base - pad_size_pix, i_x_base + base_size_pix + pad_size_pix,
                    i_x_base - pad_size_pix, i_x_base + base_size_pix + pad_size_pix,
                ],
                [
                    i_y_base - pad_size_pix, i_y_base + base_size_pix + pad_size_pix,
                    i_y_base - pad_size_pix, i_y_base + base_size_pix + pad_size_pix,
                ],
            ]
        )
    )

    bbox = {
        "left": min(x),
        "right": max(x),
        "top": max(y),
        "bottom": min(y),
    }

    return bbox
