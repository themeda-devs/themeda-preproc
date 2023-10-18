import enum
import types

import numpy as np

import rasterio.enums


class DataSourceName(enum.Enum):
    LAND_COVER = "land_cover"
    TMAX = "tmax"
    RAIN = "rain"
    ELEVATION = "elevation"
    LAND_USE = "land_use"
    FIRE_SCAR_EARLY = "fire_scar_early"
    FIRE_SCAR_LATE = "fire_scar_late"
    SOIL_ECE = "soil_ece"
    SOIL_DEPTH = "soil_depth"
    SOIL_CLAY = "soil_clay"

    def __str__(self) -> str:
        return str(self.value)


DATA_SOURCE_HANDLER = types.MappingProxyType(
    {
        DataSourceName("land_cover"): "land_cover",
        DataSourceName("tmax"): "climate",
        DataSourceName("rain"): "climate",
        DataSourceName("elevation"): "elevation",
        DataSourceName("land_use"): "land_use",
        DataSourceName("fire_scar_early"): "fire_scar",
        DataSourceName("fire_scar_late"): "fire_scar",
        DataSourceName("soil_ece"): "soil",
        DataSourceName("soil_depth"): "soil",
        DataSourceName("soil_clay"): "soil",
    }
)

DATA_SOURCE_NODATA = types.MappingProxyType(
    {
        DataSourceName("land_cover"): 0,
        DataSourceName("tmax"): np.nan,
        DataSourceName("rain"): np.nan,
        DataSourceName("elevation"): np.nan,
        DataSourceName("land_use"): 0,
        DataSourceName("fire_scar_early"): 0,
        DataSourceName("fire_scar_late"): 0,
        DataSourceName("soil_ece"): np.nan,
        DataSourceName("soil_depth"): np.nan,
        DataSourceName("soil_clay"): np.nan,
    }
)

DATA_SOURCE_SENTINEL = types.MappingProxyType(
    {
        DataSourceName("land_cover"): 255,
        DataSourceName("tmax"): np.nan,
        DataSourceName("rain"): np.nan,
        DataSourceName("elevation"): np.nan,
        DataSourceName("land_use"): 255,
        DataSourceName("fire_scar_early"): 255,
        DataSourceName("fire_scar_late"): 255,
        DataSourceName("soil_ece"): np.nan,
        DataSourceName("soil_depth"): np.nan,
        DataSourceName("soil_clay"): np.nan,
    }
)

# this is for the chiplets numpy array
DATA_SOURCE_DTYPE = types.MappingProxyType(
    {
        DataSourceName("land_cover"): np.uint8,
        DataSourceName("tmax"): np.float16,
        DataSourceName("rain"): np.float16,
        DataSourceName("elevation"): np.float16,
        DataSourceName("land_use"): np.uint8,
        DataSourceName("fire_scar_early"): np.uint8,
        DataSourceName("fire_scar_late"): np.uint8,
        DataSourceName("soil_ece"): np.float16,
        DataSourceName("soil_depth"): np.float16,
        DataSourceName("soil_clay"): np.float16,
    }
)

DATA_SOURCE_RESAMPLERS = types.MappingProxyType(
    {
        DataSourceName("land_cover"): rasterio.enums.Resampling.nearest,
        DataSourceName("tmax"): rasterio.enums.Resampling.bilinear,
        DataSourceName("rain"): rasterio.enums.Resampling.bilinear,
        DataSourceName("elevation"): rasterio.enums.Resampling.bilinear,
        DataSourceName("land_use"): rasterio.enums.Resampling.nearest,
        DataSourceName("fire_scar_early"): rasterio.enums.Resampling.nearest,
        DataSourceName("fire_scar_late"): rasterio.enums.Resampling.nearest,
        DataSourceName("soil_ece"): rasterio.enums.Resampling.bilinear,
        DataSourceName("soil_depth"): rasterio.enums.Resampling.bilinear,
        DataSourceName("soil_clay"): rasterio.enums.Resampling.bilinear,
    }
)


def is_data_source_continuous(source_name: DataSourceName) -> bool:
    return np.issubdtype(
        DATA_SOURCE_DTYPE[source_name],
        np.floating,
    )
