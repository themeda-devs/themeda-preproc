import typing
import contextlib

import numpy as np

import xarray as xr

import polars as pl

import tqdm

import shapely
import affine

import ecofuture_preproc.roi


def form_chiplet_table(
    chips: list[xr.DataArray],
    roi: ecofuture_preproc.roi.RegionOfInterest,
    pad_size_pix: int,
    base_size_pix: int = 160,
    show_progress: bool = True,
) -> pl.dataframe.frame.DataFrame:

    with contextlib.closing(
        tqdm.tqdm(
            iterable=None,
            total=len(chips),
            disable=not show_progress,
        )
    ) as progress_bar:

        items = []

        for chip in chips:

            items.append(
                form_chiplet_table_entry(
                    chip=chip,
                    roi=roi,
                    base_size_pix=base_size_pix,
                    pad_size_pix=pad_size_pix,
                )
            )

            progress_bar.update()

        table = pl.concat(items=items)

    return table



def form_chiplet_table_entry(
    chip: xr.DataArray,
    roi: ecofuture_preproc.roi.RegionOfInterest,
    base_size_pix: int,
    pad_size_pix: int,
) -> pl.dataframe.frame.DataFrame:

    chip_transform = chip.rio.transform()

    chip_grid_ref = ecofuture_preproc.chips.get_grid_ref_from_chip(chip=chip)

    schema = get_schema()

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

    roi_contains_chip = roi.shape.contains(
        other=chip.odc.geobox.boundingbox.polygon.geom
    )

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

            if not roi_contains_chip:

                base_bbox_shape = shapely.geometry.box(
                    minx=base_bbox["left"],
                    miny=base_bbox["bottom"],
                    maxx=base_bbox["right"],
                    maxy=base_bbox["top"],
                )

                shapely.prepare(base_bbox_shape)

                chiplet_intersects_roi = base_bbox_shape.intersects(other=roi.shape)

                if not chiplet_intersects_roi:
                    continue

                roi_contains_chiplet = roi.shape.contains(other=base_bbox_shape)

            else:
                roi_contains_chiplet = True

            row_data = {
                **common_columns,
                "partial_roi_overlap": not roi_contains_chiplet,
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


def get_schema() -> dict[str, typing.Union[pl.Int64, pl.UInt64, pl.Float64]]:

    schema = {
        "partial_roi_overlap": pl.Boolean,
        "chip_i_x_base": pl.UInt64,
        "chip_i_y_base": pl.UInt64,
        "chip_grid_ref_x_base": pl.Int64,
        "chip_grid_ref_y_base": pl.Int64,
        **{
            f"{pad_prefix}bbox_{pos}": pl.Int64
            for pad_prefix in ["", "pad_"]
            for pos in ["left", "bottom", "right", "top"]
        },
        **{
            f"chip_transform_i_to_coords_coeff_{letter}": pl.Float64
            for letter in "abcdefghi"
        },
    }

    return schema
