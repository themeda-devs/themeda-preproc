import enum


class DataSourceName(enum.Enum):
    LAND_COVER = "land_cover"
    TMAX = "tmax"
    RAIN = "rain"
    ELEVATION = "elevation"
    LAND_USE = "land_use"
    FIRE_SCAR = "fire_scar"

    def __str__(self) -> str:
        return str(self.value)
