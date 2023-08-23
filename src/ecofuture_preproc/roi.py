import enum
import pathlib
import typing
import dataclasses
import importlib.resources

import shapely


class ROIName(enum.Enum):
    SAVANNA = "savanna"
    AUSTRALIA = "australia"

    def __str__(self) -> str:
        return self.value


@dataclasses.dataclass
class RegionOfInterest:
    roi_name: ROIName

    # TODO

    @property
    def geojson_path(self):

        geojson_filename_lut = {
            "savanna": "NAust_mask_RoI_WGS1984.geojson",
            "australia": "Aust_coastline_mainland_WGS1984.geojson",
        }

        path = importlib.resources.files("ecofuture_preproc.resources.roi").joinpath(
            geojson_filename_lut[self.roi_name.value]
        )

        return path



def run_prep(
    roi_name: ROIName,
    base_output_dir: typing.Union[pathlib.Path, str],
    protect: bool,
    show_progress: bool,
) -> None:

    base_output_dir = pathlib.Path(base_output_dir)


def get_roi_geojson_path(
    roi_name: typing.Union[ROIName, str],
) -> pathlib.Path:

    if isinstance(roi_name, str):
        roi_name = ROIName(roi_name)

