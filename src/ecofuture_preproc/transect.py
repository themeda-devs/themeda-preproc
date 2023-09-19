import dataclasses

import numpy as np

import scipy.spatial.distance

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

    (*_, natt_roi_segment) = natt_roi_points.geoms

    ((natt_start_lon, natt_end_lon), (natt_start_lat, natt_end_lat)) = (
        natt_roi_segment.xy
    )

    transect_start = PointLatLon(
        longitude=natt_start_lon,
        latitude=natt_start_lat,
    )
    transect_end = PointLatLon(
        longitude=natt_end_lon,
        latitude=natt_end_lat,
    )

    (transect_start_albers, transect_end_albers) = (
        ecofuture_preproc.utils.transform_shape(
            src_crs=4326,
            dst_crs=3577,
            shape=shapely.Point(transect_point.longitude, transect_point.latitude),
        )
        for transect_point in [transect_start, transect_end]
    )

    transect_length = scipy.spatial.distance.euclidean(
        u=[transect_start_albers.x, transect_start_albers.y],
        v=[transect_end_albers.x, transect_end_albers.y],
    )

    pixel_length = 25

    n_points = round(transect_length / pixel_length)

    points = np.linspace(
        start=[transect_start_albers.x, transect_start_albers.y],
        stop=[transect_end_albers.x, transect_end_albers.y],
        num=n_points,
        endpoint=True,
    )

    x = xr.DataArray(
        data=points[:, 0],
        dims="transect",
    )
    y = xr.DataArray(
        data=points[:, 1],
        dims="transect",
    )

    xy = xr.Dataset(
        {"x": x, "y": y},
    )

    return xy

