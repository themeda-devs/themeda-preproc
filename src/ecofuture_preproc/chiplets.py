
import xarray as xr

import polars as pl

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

    for chip_i_x_base in range(0, chip.sizes["x"], base_size_pix):
        for chip_i_y_base in range(0, chip.sizes["y"], base_size_pix):

            i_base_x = np.arange(chip_i_x_base, chip_i_x_base + base_size_pix)
            i_base_y = np.arange(chip_i_y_base, chip_i_y_base + base_size_pix)

            # TODO

    return schema
