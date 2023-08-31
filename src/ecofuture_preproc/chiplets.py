
import numpy as np

import xarray as xr

import affine


def get_chiplet_from_packet(
    packet: xr.DataArray,
    chip_i_x_base: int,
    chip_i_y_base: int,
    chip_i_to_coords_transform: affine.Affine,
    base_size_pix: int,
    pad_size_pix: int,
) -> xr.DataArray:

    i_x = np.arange(
        chip_i_x_base - pad_size_pix,
        chip_i_x_base + base_size_pix + pad_size_pix,
    ) + 0.5
    i_y = np.arange(
        chip_i_y_base - pad_size_pix,
        chip_i_y_base + base_size_pix + pad_size_pix,
    ) + 0.5

    i_xy = np.row_stack((i_x, i_y))

    (x, y) = chip_i_to_coords_transform * i_xy

    chiplet = packet.sel(x=x, y=y)

    return chiplet
