import pathlib
import typing
import multiprocessing
import multiprocessing.synchronize
import functools
import dataclasses

import numpy as np
import numpy.typing as npt

import xarray as xr

import polars as pl

import affine

import tqdm

import ecofuture_preproc.source
import ecofuture_preproc.roi
import ecofuture_preproc.chips
import ecofuture_preproc.packet
import ecofuture_preproc.chiplet_table
import ecofuture_preproc.utils


@dataclasses.dataclass(frozen=True)
class ChipletFilenameInfo:
    path: pathlib.Path
    roi_name: ecofuture_preproc.roi.ROIName
    source_name: ecofuture_preproc.source.DataSourceName
    pad_size_pix: int
    year: int


def form_chiplets(
    table: pl.dataframe.frame.DataFrame,
    source_name: ecofuture_preproc.source.DataSourceName,
    roi: ecofuture_preproc.roi.RegionOfInterest,
    base_size_pix: int,
    pad_size_pix: int,
    base_output_dir: pathlib.Path,
    protect: bool,
    cores: int,
    relabeller: typing.Optional[
        typing.Callable[[xr.DataArray, bool], xr.DataArray]
    ] = None,
    show_progress: bool = True,
    load_chips_masked: bool = False,
    form_packet_via_rioxarray: bool = True,
) -> None:
    source_chip_dir = (
        base_output_dir / "chips" / f"roi_{roi.name.value}" / source_name.value
    )

    years = ecofuture_preproc.utils.get_years_in_path(path=source_chip_dir)

    with multiprocessing.Manager() as manager:
        lock = manager.RLock()

        func = functools.partial(
            form_year_chiplets,
            table=table,
            source_name=source_name,
            roi=roi,
            base_size_pix=base_size_pix,
            pad_size_pix=pad_size_pix,
            base_output_dir=base_output_dir,
            protect=protect,
            show_progress=show_progress,
            relabeller=relabeller,
            load_chips_masked=load_chips_masked,
            form_packet_via_rioxarray=form_packet_via_rioxarray,
        )

        with multiprocessing.Pool(
            processes=cores,
            initializer=tqdm.tqdm.set_lock,
            initargs=(lock,),
        ) as pool:
            pool.starmap(func, enumerate(years), chunksize=1)


def form_year_chiplets(
    progress_bar_position: int,
    year: int,
    table: pl.dataframe.frame.DataFrame,
    source_name: ecofuture_preproc.source.DataSourceName,
    roi: ecofuture_preproc.roi.RegionOfInterest,
    base_size_pix: int,
    pad_size_pix: int,
    base_output_dir: pathlib.Path,
    protect: bool,
    show_progress: bool,
    relabeller: typing.Optional[
        typing.Callable[[xr.DataArray, bool], xr.DataArray]
    ] = None,
    load_chips_masked: bool = False,
    form_packet_via_rioxarray: bool = False,
) -> None:

    progress_bar = tqdm.tqdm(
        iterable=None,
        total=len(table),
        disable=not show_progress,
        position=progress_bar_position,
        desc=str(year),
        leave=False,
        dynamic_ncols=True,
    )

    chip_dir = (
        base_output_dir
        / "chips"
        / f"roi_{roi.name.value}"
        / source_name.value
        / str(year)
    )

    chip_paths = sorted(chip_dir.glob("*.tif"))

    if len(chip_paths) == 0:
        raise ValueError(f"No chips found in {chip_dir}")

    output_path = get_chiplet_path(
        source_name=source_name,
        year=year,
        roi_name=ecofuture_preproc.roi.ROIName(roi.name),
        pad_size_pix=pad_size_pix,
        base_output_dir=base_output_dir,
    )

    if ecofuture_preproc.utils.is_path_existing_and_read_only(path=output_path):
        return

    packet = ecofuture_preproc.packet.form_packet(
        paths=chip_paths,
        form_via_rioxarray=form_packet_via_rioxarray,
        nodata=ecofuture_preproc.source.DATA_SOURCE_NODATA[source_name],
        load_chips_masked=load_chips_masked,
    )

    chiplets = np.memmap(
        filename=output_path,
        dtype=ecofuture_preproc.source.DATA_SOURCE_DTYPE[source_name],
        mode="w+",
        shape=get_array_shape(
            table=table,
            base_size_pix=base_size_pix,
            pad_size_pix=pad_size_pix,
        ),
    )

    transforms: dict[ecofuture_preproc.chips.GridRef, affine.Affine] = {}

    for row in table.iter_rows(named=True):
        grid_ref = ecofuture_preproc.chips.GridRef(
            x=row["chip_grid_ref_x_base"],
            y=row["chip_grid_ref_y_base"],
        )

        if grid_ref not in transforms:
            transforms[grid_ref] = get_transform_from_row(row=row)

        chiplet = get_chiplet_from_packet(
            packet=packet,
            chip_i_x_base=row["chip_i_x_base"],
            chip_i_y_base=row["chip_i_y_base"],
            chip_i_to_coords_transform=transforms[grid_ref],
            base_size_pix=base_size_pix,
            pad_size_pix=pad_size_pix,
        )

        # do relabellin'
        if relabeller is not None:
            try:
                chiplet = relabeller(
                    chiplet,
                    row["partial_roi_overlap"],
                )
            except ValueError:
                print(f"Error in year {year}")
                print(f"Error with chiplet: {row}")
                print(f"Index: {row['index']}")
                raise

        chiplets[row["index"], ...] = chiplet.values

        chiplet.close()

        del chiplet

        progress_bar.update()

    # write changes to disk
    chiplets.flush()

    # close the handle
    # see https://github.com/numpy/numpy/issues/13510
    chiplets._mmap.close()  # type: ignore

    if protect:
        ecofuture_preproc.utils.protect_path(path=output_path)

    packet.close()

    del packet

    progress_bar.close()


def load_chiplets(
    source_name: ecofuture_preproc.source.DataSourceName,
    year: int,
    roi_name: ecofuture_preproc.roi.ROIName,
    pad_size_pix: int,
    base_output_dir: pathlib.Path,
    base_size_pix: int = 160,
) -> np.memmap:  # type: ignore
    table = ecofuture_preproc.chiplet_table.load_table(
        roi_name=roi_name,
        base_output_dir=base_output_dir,
        pad_size_pix=pad_size_pix,
    )

    chiplet_path = get_chiplet_path(
        source_name=source_name,
        year=year,
        roi_name=roi_name,
        pad_size_pix=pad_size_pix,
        base_output_dir=base_output_dir,
    )

    chiplet = np.memmap(
        filename=chiplet_path,
        dtype=ecofuture_preproc.source.DATA_SOURCE_DTYPE[source_name],
        mode="r",
        shape=get_array_shape(
            table=table,
            base_size_pix=base_size_pix,
            pad_size_pix=pad_size_pix,
        ),
    )

    return chiplet


def get_array_shape(
    table: pl.dataframe.frame.DataFrame,
    base_size_pix: int,
    pad_size_pix: int,
) -> tuple[int, int, int]:
    shape = (len(table),) + ((base_size_pix + pad_size_pix * 2),) * 2

    return shape


def get_chiplet_path(
    source_name: ecofuture_preproc.source.DataSourceName,
    year: int,
    roi_name: ecofuture_preproc.roi.ROIName,
    pad_size_pix: int,
    base_output_dir: pathlib.Path,
) -> pathlib.Path:
    chiplet_dir: pathlib.Path = (
        base_output_dir
        / "chiplets"
        / f"roi_{roi_name.value}"
        / f"pad_{pad_size_pix}"
        / source_name.value
    )

    chiplet_dir.mkdir(exist_ok=True, parents=True)

    chiplet_path = chiplet_dir / (
        f"chiplets_{source_name.value}_{year}_"
        + f"roi_{roi_name.value}_pad_{pad_size_pix}.npy"
    )

    return chiplet_path


def parse_chiplet_filename(filename: pathlib.Path) -> ChipletFilenameInfo:
    components = filename.stem.split("_")

    pad_size_pix = int(components.pop())
    assert components.pop() == "pad"
    roi_name = ecofuture_preproc.roi.ROIName(components.pop())
    assert components.pop() == "roi"
    year = int(components.pop())
    assert components[0] == "chiplets"
    source_name = ecofuture_preproc.source.DataSourceName("_".join(components[1:]))

    return ChipletFilenameInfo(
        path=filename,
        roi_name=roi_name,
        source_name=source_name,
        year=year,
        pad_size_pix=pad_size_pix,
    )


def get_transform_from_row(row: dict[str, typing.Any]) -> affine.Affine:
    return affine.Affine(
        **{
            letter: row[f"chip_transform_i_to_coords_coeff_{letter}"]
            for letter in "abcdefghi"
        }
    )


def get_chiplet_from_packet(
    packet: xr.DataArray,
    chip_i_x_base: int,
    chip_i_y_base: int,
    chip_i_to_coords_transform: affine.Affine,
    base_size_pix: int,
    pad_size_pix: int,
) -> xr.DataArray:
    i_x = (
        np.arange(
            chip_i_x_base - pad_size_pix,
            chip_i_x_base + base_size_pix + pad_size_pix,
        )
        + 0.5
    )
    i_y = (
        np.arange(
            chip_i_y_base - pad_size_pix,
            chip_i_y_base + base_size_pix + pad_size_pix,
        )
        + 0.5
    )

    i_xy = np.row_stack((i_x, i_y))

    (x, y) = chip_i_to_coords_transform * i_xy

    chiplet = packet.sel(x=x, y=y)

    return chiplet


def convert_chiplet_to_data_array(
    chiplet: npt.NDArray,
    metadata: dict[str, typing.Any],
    pad_size_pix: int,
    base_size_pix: int = 160,
    crs: int = 3577,
    nodata: typing.Union[int, float, np.float32] = 0,
    new_resolution: typing.Optional[typing.Union[int, float]] = None,
) -> xr.DataArray:
    # first, remove any padding from the raw data
    chiplet = chiplet[
        pad_size_pix : (base_size_pix + pad_size_pix),
        pad_size_pix : (base_size_pix + pad_size_pix),
    ]

    (n_y, n_x) = chiplet.shape

    if not ((n_x == n_y) and n_x == base_size_pix):
        raise ValueError("Unexpected chiplet dimensions")

    # this goes from the index space of the *chip* to coordinates
    transform = get_transform_from_row(row=metadata)

    i_chip_x = np.arange(
        metadata["chip_i_x_base"],
        metadata["chip_i_x_base"] + base_size_pix,
    )
    i_chip_y = np.arange(
        metadata["chip_i_y_base"],
        metadata["chip_i_y_base"] + base_size_pix,
    )

    i_xy = np.row_stack((i_chip_x + 0.5, i_chip_y + 0.5))

    (x, y) = transform * i_xy

    data = xr.DataArray(
        data=chiplet,
        dims=("y", "x"),
        coords={"x": x, "y": y},
    )

    data.rio.set_crs(input_crs=crs, inplace=True)
    data.rio.set_nodata(input_nodata=nodata, inplace=True)

    data = data.odc.assign_crs(crs=data.rio.crs)

    if new_resolution is not None:
        data = data.odc.reproject(
            how=data.odc.crs,
            resolution=new_resolution,
        )

    return data
