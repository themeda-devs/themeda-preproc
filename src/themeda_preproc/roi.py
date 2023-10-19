# allows methods in the class to have variables of the same type as the class
from __future__ import annotations

import enum
import pathlib
import typing
import importlib.resources
import pickle

import shapely

import themeda_preproc.utils


class ROIName(enum.Enum):
    SAVANNA = "savanna"
    AUSTRALIA = "australia"

    def __str__(self) -> str:
        return self.value


class RegionOfInterest:
    __slots__ = ("name", "_shape", "base_output_dir")

    _geojson_filename_lut: typing.ClassVar = {
        "savanna": "NAust_mask_RoI_WGS1984.geojson",
        "australia": "Aust_coastline_mainland_WGS1984.geojson",
    }

    def __init__(
        self,
        name: ROIName,
        base_output_dir: typing.Optional[pathlib.Path] = None,
        load: bool = True,
    ) -> None:
        self.name = name

        self.base_output_dir = base_output_dir

        if load:
            self.load()
        else:
            self._shape = None

    @property
    def shape(self) -> shapely.geometry.polygon.Polygon:
        if self._shape is None:
            raise ValueError("Need to run `.load()` first")
        return self._shape

    @property
    def shape_path(self) -> pathlib.Path:
        if self.base_output_dir is None:
            raise ValueError("Need to set `base_output_dir`")
        return self.base_output_dir / "roi" / f"themeda_roi_shape_{self.name.value}.pkl"

    @property
    def geojson_path(self) -> pathlib.Path:
        path = pathlib.Path(
            str(
                importlib.resources.files("themeda_preproc.resources.roi").joinpath(
                    self._geojson_filename_lut[self.name.value]
                )
            )
        )

        return path

    def intersects_with(self, other_roi: RegionOfInterest) -> bool:
        "'Returns True if A and B share any portion of space'"
        intersects: bool = self.shape.intersects(other=other_roi.shape)
        return intersects

    def is_within(self, other_roi: RegionOfInterest) -> bool:
        "'Returns True if geometry A is completely inside geometry B'"
        within: bool = self.shape.within(other=other_roi.shape)
        return within

    def prepare(
        self,
        src_crs: int = 4326,
        dst_crs: int = 3577,
        save: bool = True,
    ) -> None:
        """
        Loads a spatial region-of-interest from a GeoJSON file, optionally converts the
        projection, and formats the region as a `shapely` Polygon.
        """

        geoms = shapely.from_geojson(geometry=self.geojson_path.read_text())

        try:
            (region,) = geoms.geoms
        except ValueError as err:
            raise ValueError(
                "Unexpected number of components in the region file"
            ) from err

        if src_crs != dst_crs:
            region = themeda_preproc.utils.transform_shape(
                src_crs=src_crs,
                dst_crs=dst_crs,
                shape=region,
            )

        # might speed up calls that use the geometry
        shapely.prepare(geometry=region)

        if save:
            self.shape_path.parent.mkdir(parents=True, exist_ok=True)

            with self.shape_path.open("wb") as handle:
                pickle.dump(region, handle)

            self.load()

        else:
            self._shape = region

    def load(self) -> None:
        with self.shape_path.open("rb") as handle:
            self._shape = pickle.load(handle)


def run(
    roi_name: ROIName,
    base_output_dir: pathlib.Path,
    protect: bool,
) -> None:
    roi = RegionOfInterest(
        name=roi_name,
        base_output_dir=base_output_dir,
        load=False,
    )

    roi.prepare()

    if protect:
        themeda_preproc.utils.protect_path(path=roi.shape_path)
