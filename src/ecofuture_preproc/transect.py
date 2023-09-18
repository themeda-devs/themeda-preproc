import dataclasses

import xarray as xr

import shapely

import ecofuture_preproc.roi
import ecofuture_preproc.utils


@dataclasses.dataclass
class PointLatLon:
    latitude: float
    longitude: float


def get_natt_coords(
    roi: ecofuture_preproc.roi.RegionOfInterest,
) -> xr.Dataset:

    roi_shape_latlon = ecofuture_preproc.utils.transform_shape(
        src_crs=3577,
        dst_crs=4326,
        shape=roi.shape,
    )

    natt_start = PointLatLon(
        longitude=130.8410469,
        latitude=-12.46044,
    )

    natt_end = PointLatLon(
        longitude=134.1899847,
        latitude=-22,
    )

    natt = shapely.LineString(
        coordinates=[
            (natt_start.longitude, natt_start.latitude),
            (natt_end.longitude, natt_end.latitude),
        ],
    )

    natt_roi_points = roi_shape_latlon.intersection(other=natt)

    (natt_left, natt_bottom, natt_right, natt_top) = natt_roi_points.bounds

    transect_start = PointLatLon(
        longitude=natt_left,
        latitude=natt_top,
    )
    transect_end = PointLatLon(
        longitude=natt_right,
        latitude=natt_bottom,
    )

    # TODO
    # - convert transect points to Albers
    # - make gridded points along transect
    # - make DataArrays for indexing into the packet

    return (transect_start, transect_end)
