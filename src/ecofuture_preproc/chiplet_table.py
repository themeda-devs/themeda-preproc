import typing
import contextlib
import collections
import pathlib
import os
import types

import numpy as np

import xarray as xr

import polars as pl

import tqdm

import shapely
import affine

import ecofuture_preproc.roi
import ecofuture_preproc.chips


def run(
    roi_name: ecofuture_preproc.roi.ROIName,
    base_output_dir: pathlib.Path,
    pad_size_pix: int,
    protect: bool = True,
    show_progress: bool = True,
) -> None:
    table_path = get_table_path(
        roi_name=roi_name,
        base_output_dir=base_output_dir,
        pad_size_pix=pad_size_pix,
    )

    # if it already exists and is protected, bail
    if table_path.exists() and not os.access(table_path, os.W_OK):
        print(f"Path {table_path} exists and is protected; skipping")
        return

    roi = ecofuture_preproc.roi.RegionOfInterest(
        name=roi_name,
        base_output_dir=base_output_dir,
        load=True,
    )

    rand_seed = get_rand_seed(
        roi_name=roi_name,
        pad_size_pix=pad_size_pix,
    )

    ref_data_source = "land_cover"

    ref_chips_base_dir = (
        base_output_dir / "chips" / f"roi_{roi_name.value}" / ref_data_source
    )

    (ref_year, *_) = sorted(
        [
            ref_chip_year_path
            for ref_chip_year_path in ref_chips_base_dir.glob("*")
            if ref_chip_year_path.is_dir()
        ]
    )

    ref_chips_dir = ref_chips_base_dir / ref_year

    ref_chips_paths = sorted(
        [ref_chip_path for ref_chip_path in ref_chips_dir.glob("*.tif")]
    )

    ref_chips: list[xr.DataArray] = [
        ecofuture_preproc.chips.read_chip(
            path=ref_chip_path,
            chunks="auto",
            load_data=False,
        )
        for ref_chip_path in ref_chips_paths
    ]

    table = form_chiplet_table(
        chips=ref_chips,
        roi=roi,
        pad_size_pix=pad_size_pix,
        rand_seed=rand_seed,
        show_progress=show_progress,
    )

    table.write_parquet(file=table_path)

    if protect:
        ecofuture_preproc.utils.protect_path(path=table_path)


def get_table_path(
    roi_name: ecofuture_preproc.roi.ROIName,
    base_output_dir: pathlib.Path,
    pad_size_pix: int,
) -> pathlib.Path:
    table_dir = (
        base_output_dir
        / "chiplet_table"
        / f"roi_{roi_name.value}"
        / f"pad_{pad_size_pix}"
    )

    table_dir.mkdir(parents=True, exist_ok=True)

    table_path = (
        table_dir / f"chiplet_table_roi_{roi_name.value}_pad_{pad_size_pix}.parquet"
    )

    return table_path


def load_table(
    roi_name: ecofuture_preproc.roi.ROIName,
    base_output_dir: pathlib.Path,
    pad_size_pix: int,
) -> pl.dataframe.frame.DataFrame:
    table_path = get_table_path(
        roi_name=roi_name,
        base_output_dir=base_output_dir,
        pad_size_pix=pad_size_pix,
    )

    table = pl.read_parquet(source=table_path)

    return table


def form_chiplet_table(
    chips: list[xr.DataArray],
    roi: ecofuture_preproc.roi.RegionOfInterest,
    pad_size_pix: int,
    rand_seed: int,
    base_size_pix: int = 160,
    n_spatial_subsets: int = 5,
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

    index = np.arange(len(table))

    rand = np.random.RandomState(rand_seed)

    subset_num = rand.choice(
        a=np.arange(1, n_spatial_subsets + 1),
        size=len(table),
    )

    subset_instance_counter: collections.Counter[int] = collections.Counter()

    subset_instance_num = []

    for row_subset_num in subset_num:
        subset_instance_counter[row_subset_num] += 1
        subset_instance_num.append(subset_instance_counter[row_subset_num])

    table = table.with_columns(
        index=pl.Series(values=index),
        subset_num=pl.Series(values=subset_num),
        subset_instance_num=pl.Series(values=subset_instance_num),
    )

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
                    for (bbox, pad_prefix) in zip(
                        (base_bbox, padded_bbox), ["", "pad_"]
                    )
                    for pos in ["left", "bottom", "right", "top"]
                },
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
    (x, y) = transform * np.array(
        [
            [
                i_x_base - pad_size_pix,
                i_x_base + base_size_pix + pad_size_pix,
                i_x_base - pad_size_pix,
                i_x_base + base_size_pix + pad_size_pix,
            ],
            [
                i_y_base - pad_size_pix,
                i_y_base + base_size_pix + pad_size_pix,
                i_y_base - pad_size_pix,
                i_y_base + base_size_pix + pad_size_pix,
            ],
        ]
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


def get_rand_seed(roi_name: ecofuture_preproc.roi.ROIName, pad_size_pix: int) -> int:
    rand_seed_lut = types.MappingProxyType(
        {
            ("savanna", 32): 845627234,
            ("savanna", 0): 845627234,
            ("australia", 32): 587902257,
        }
    )

    return rand_seed_lut[(roi_name.value, pad_size_pix)]
