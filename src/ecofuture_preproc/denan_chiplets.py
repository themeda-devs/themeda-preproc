import pathlib
import multiprocessing
import multiprocessing.synchronize
import functools

import numpy as np

import tqdm

import ecofuture_preproc.source
import ecofuture_preproc.roi
import ecofuture_preproc.chips
import ecofuture_preproc.packet
import ecofuture_preproc.utils


def run(
    source_name: ecofuture_preproc.source.DataSourceName,
    roi_name: ecofuture_preproc.roi.ROIName,
    pad_size_pix: int,
    base_output_dir: pathlib.Path,
    protect: bool,
    cores: int,
    show_progress: bool = True,
) -> None:

    if not ecofuture_preproc.source.is_data_source_continuous(source_name=source_name):
        raise ValueError("Only useful to run this on float data types")

    chiplet_base_dir = (
        base_output_dir
        / "chiplets"
        / f"roi_{roi_name.value}"
        / f"pad_{pad_size_pix}"
        / source_name.value
    )

    chiplets_file_info = [
        ecofuture_preproc.chiplets.parse_chiplet_filename(filename=chiplet_path)
        for chiplet_path in sorted(chiplet_base_dir.glob("*.npy"))
    ]

    years = sorted([chiplet_file_info.year for chiplet_file_info in chiplets_file_info])

    # see https://pola-rs.github.io/polars-book/user-guide/misc/multiprocessing/
    mp = multiprocessing.get_context(method="spawn")

    with mp.Manager() as manager:
        lock = manager.RLock()

        func = functools.partial(
            run_denan_year_chiplets,
            base_output_dir=base_output_dir,
            source_name=source_name,
            roi_name=roi_name,
            pad_size_pix=pad_size_pix,
            protect=protect,
            show_progress=show_progress,
        )

        with mp.Pool(
            processes=cores,
            initializer=tqdm.tqdm.set_lock,
            initargs=(lock,),
        ) as pool:
            pool.starmap(func, enumerate(years), chunksize=1)


def run_denan_year_chiplets(
    progress_bar_position: int,
    year: int,
    source_name: ecofuture_preproc.source.DataSourceName,
    roi_name: ecofuture_preproc.roi.ROIName,
    pad_size_pix: int,
    base_output_dir: pathlib.Path,
    protect: bool,
    show_progress: bool,
) -> None:

    progress_bar = tqdm.tqdm(
        iterable=None,
        disable=not show_progress,
        position=progress_bar_position,
        desc=str(year),
        leave=False,
        dynamic_ncols=True,
    )

    output_path = ecofuture_preproc.chiplets.get_chiplet_path(
        source_name=source_name,
        year=year,
        roi_name=roi_name,
        pad_size_pix=pad_size_pix,
        base_output_dir=base_output_dir,
        denan=True,
    )

    if ecofuture_preproc.utils.is_path_existing_and_read_only(path=output_path):
        return

    # load the original chiplets
    with ecofuture_preproc.chiplets.chiplets_reader(
        source_name=source_name,
        year=year,
        roi_name=roi_name,
        pad_size_pix=pad_size_pix,
        base_output_dir=base_output_dir,
        denan=False,
    ) as orig_chiplets:

        # initialise the new memmap
        denan_chiplets = np.memmap(
            filename=output_path,
            dtype=orig_chiplets.dtype,
            mode="w+",
            shape=orig_chiplets.shape,
        )

        (n_chiplets, _, _) = orig_chiplets.shape

        progress_bar.total = n_chiplets
        progress_bar.refresh()

        for i_chiplet in range(n_chiplets):

            data = np.array(orig_chiplets[i_chiplet, ...])

            isnan_data = np.isnan(data)

            # if there are any nans in the data, need to replace them
            if np.any(np.isnan(data)):

                if np.all(isnan_data):

                    if source_name not in [
                        ecofuture_preproc.source.DataSourceName.SOIL_DEPTH,
                        ecofuture_preproc.source.DataSourceName.SOIL_ECE,
                        ecofuture_preproc.source.DataSourceName.SOIL_CLAY,
                    ]:
                        raise ValueError(
                            "Only expecting to see full nan chiplets in the soil variables"
                        )

                    fill_val = np.float16(0.0)

                else:

                    # use the mean of the non-nan values as the fill value
                    # cast to a regular float first to avoid precision issues
                    fill_val = np.nanmean(data.astype(float))

                # replace nans in-place with the fill value
                np.nan_to_num(
                    x=data,
                    copy=False,
                    nan=fill_val.astype(np.float16),
                )

            # set the new data in the memmapped array
            denan_chiplets[i_chiplet, ...] = data

            progress_bar.update()

        # write changes to disk
        denan_chiplets.flush()

        # close the handle
        assert hasattr(denan_chiplets, "_mmap")
        denan_chiplets._mmap.close()

    if protect:
        ecofuture_preproc.utils.protect_path(path=output_path)

    progress_bar.close()
