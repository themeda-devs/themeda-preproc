import pytest

import shapely

import ecofuture_preproc.roi


@pytest.fixture(scope="session")
def roi_prep(base_output_dir):

    for roi_name in ecofuture_preproc.roi.ROIName:
        ecofuture_preproc.roi.run(
            roi_name=roi_name,
            base_output_dir=base_output_dir,
            protect=True,
        )


@pytest.mark.usefixtures("roi_prep")
def test_savanna_roi(base_output_dir):

    roi_name = ecofuture_preproc.roi.ROIName("savanna")

    roi = ecofuture_preproc.roi.RegionOfInterest(
        name=roi_name,
        base_output_dir=base_output_dir,
    )
