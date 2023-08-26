import enum


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


DATA_SOURCE_HANDLER = {
    DataSourceName("land_cover"): "dea",
    DataSourceName("tmax"): "climate",
    DataSourceName("rain"): "climate",
    DataSourceName("elevation"): "elevation",
    DataSourceName("land_use"): "land_use",
    DataSourceName("fire_scar_early"): "fire_scar",
    DataSourceName("fire_scar_late"): "fire_scar",
}
