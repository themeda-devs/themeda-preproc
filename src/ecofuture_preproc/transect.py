import dataclasses
import pathlib

import numpy as np

import scipy.spatial.distance

import xarray as xr

import shapely

import ecofuture_preproc.roi
import ecofuture_preproc.utils
import ecofuture_preproc.source
import ecofuture_preproc.packet


@dataclasses.dataclass
class PointLatLon:
    latitude: float
    longitude: float


def run(
    source_name: ecofuture_preproc.source.DataSourceName,
    roi_name: ecofuture_preproc.roi.ROIName,
    base_output_dir: pathlib.Path,
    protect: bool = True,
) -> None:

    roi = ecofuture_preproc.roi.RegionOfInterest(
        roi_name=roi_name,
        base_output_dir=base_output_dir,
    )

    natt_coords = get_natt_coords(roi=roi)

    chiplet_base_dir = (
        base_output_dir
        / "chiplets_geotiff"
        / f"roi_{roi_name.value}"
        / source_name.value
    )

    years = ecofuture_preproc.utils.get_years_in_path(path=chiplet_base_dir)

    transects = []

    for year in years:

        year_chiplet_base_dir = chiplet_base_dir / str(year)

        chiplet_paths = sorted(year_chiplet_base_dir.glob("*.tif"))

        packet = ecofuture_preproc.packet.get_packet(
            paths=chiplet_paths,
            form_via_rioxarray=True,
            chunks=None,
        )

        transect = packet.sel(x=natt_coords.x, y=natt_coords.y)

        transects.append(
            xr.DataArray(
                data=transect.values[np.newaxis, :],
                dims=("year", "distance"),
                coords={
                    "year": np.array(year, ndmin=1),
                    "distance": natt_coords.distance.values,
                },
            ),
        )

        transect.close()
        packet.close()

    return transects



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

    (
        (natt_start_lon, natt_end_lon),
        (natt_start_lat, natt_end_lat),
    ) = natt_roi_segment.xy

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

    distances = xr.DataArray(
        data=np.array(
            [
                scipy.spatial.distance.euclidean(
                    u=points[0, :],
                    v=points[i_point, :],
                )
                for i_point in range(n_points)
            ]
        ),
        dims="transect",
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
        {"x": x, "y": y, "distance": distances},
    )

    return xy
