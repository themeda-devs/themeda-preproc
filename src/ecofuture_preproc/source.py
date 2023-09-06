import enum
import types

import numpy as np


class DataSourceName(enum.Enum):
    LAND_COVER = "land_cover"
    TMAX = "tmax"
    RAIN = "rain"
    ELEVATION = "elevation"
    LAND_USE = "land_use"
    FIRE_SCAR_EARLY = "fire_scar_early"
    FIRE_SCAR_LATE = "fire_scar_late"

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
    }
)

DATA_SOURCE_NODATA = types.MappingProxyType(
    {
        DataSourceName("land_cover"): 0,
        DataSourceName("tmax"): np.nan,
        DataSourceName("rain"): np.nan,
        DataSourceName("elevation"): np.nan,
        DataSourceName("land_use"): 0,
        #DataSourceName("fire_scar_early"): "fire_scar",
        #DataSourceName("fire_scar_late"): "fire_scar",
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
        #DataSourceName("fire_scar_early"): "fire_scar",
        #DataSourceName("fire_scar_late"): "fire_scar",
    }
)
