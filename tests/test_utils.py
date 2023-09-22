import pytest

import shapely

import ecofuture_preproc.utils


def test_get_years_in_path(tmp_path):

    # should error because there are no years on the path
    with pytest.raises(ValueError):
        ecofuture_preproc.utils.get_years_in_path(path=tmp_path)

    # unless silenced
    ecofuture_preproc.utils.get_years_in_path(path=tmp_path, error_if_no_years=False)

    ok_years = [1988, 1990, 1991, 2005]

    for year in ok_years:
        (tmp_path / str(year)).mkdir()

    inferred_years = ecofuture_preproc.utils.get_years_in_path(path=tmp_path)

    assert inferred_years == ok_years

    test_file = tmp_path / "test.txt"

    test_file.touch()

    # should error because there is another file in the path
    with pytest.raises(ValueError):
        ecofuture_preproc.utils.get_years_in_path(path=tmp_path)

    # unless silenced
    inferred_years = ecofuture_preproc.utils.get_years_in_path(
        path=tmp_path, error_if_other_files=False
    )

    assert inferred_years == ok_years

    (tmp_path / "2005a").mkdir()

    inferred_years = ecofuture_preproc.utils.get_years_in_path(
        path=tmp_path, error_if_other_files=False
    )

    assert inferred_years == ok_years

    test_year_file = tmp_path / "2001"
    test_year_file.touch()

    inferred_years = ecofuture_preproc.utils.get_years_in_path(
        path=tmp_path, error_if_other_files=False
    )

    assert inferred_years == ok_years

def test_num_str_to_int():

    # OK
    assert ecofuture_preproc.utils.num_str_to_int("5") == 5
    assert ecofuture_preproc.utils.num_str_to_int("5.") == 5
    assert ecofuture_preproc.utils.num_str_to_int("-5") == -5

    with pytest.raises(ValueError):
        ecofuture_preproc.utils.num_str_to_int("five")
    with pytest.raises(ValueError):
        ecofuture_preproc.utils.num_str_to_int("5.5")


def test_transform_shape():

    example_4326 = shapely.Point(130.7263181, -19.3940681)

    # from https://epsg.io/transform#s_srs=4326&t_srs=3577&x=130.7263181&y=-19.3940681
    expected_3577 = shapely.Point(-133346.94828722897, -2077422.1437344416)

    converted = ecofuture_preproc.utils.transform_shape(
        src_crs=4326,
        dst_crs=3577,
        shape=example_4326,
    )

    assert converted.equals_exact(other=expected_3577, tolerance=10)

    backwards = ecofuture_preproc.utils.transform_shape(
        src_crs=3577,
        dst_crs=4326,
        shape=converted,
    )

    assert backwards.equals_exact(other=example_4326, tolerance=10)
