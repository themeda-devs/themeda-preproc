"""
Acquires the ANU climate data (rain, tmax) by downloading the NetCDF files for all
years and months.
"""

import pathlib
import contextlib
import typing

import siphon.catalog

import tqdm

import themeda_preproc.source
import themeda_preproc.utils


def run(
    source_name: themeda_preproc.source.DataSourceName,
    base_output_dir: pathlib.Path,
    protect: bool = True,
    show_progress: bool = True,
) -> None:
    # only consider this and subsequent years
    start_year: typing.Final = 1988

    output_dir = base_output_dir / "raw" / source_name.value

    catalog_url = "https://dapds00.nci.org.au/thredds/catalog.xml"

    catalog = siphon.catalog.TDSCatalog(catalog_url=catalog_url)

    for ref_name in [
        "ANUClimate",
        "v2-0",
        "files",
        "stable",
        "month",
        source_name.value,
    ]:
        catalog = catalog.catalog_refs[ref_name].follow()

    to_download = {}

    for year, year_data in catalog.catalog_refs.items():
        if len(year) != 4 or not year.isnumeric():
            raise ValueError(f"Unexpected year ({year})")

        # skip if the year is earlier than we're interested in
        if int(year) < start_year:
            continue

        year_catalog = year_data.follow()

        for dataset_name, dataset in year_catalog.datasets.items():
            if not dataset_name.endswith(".nc"):
                raise ValueError(f"Unexpected dataset name ({dataset_name})")

            local_path = output_dir / dataset_name

            if not local_path.exists():
                to_download[dataset] = local_path

    with contextlib.closing(
        tqdm.tqdm(
            iterable=None,
            total=len(to_download),
            disable=not show_progress,
        )
    ) as progress_bar:
        for dataset, local_path in to_download.items():
            dataset.download(filename=str(local_path))

            if protect:
                themeda_preproc.utils.protect_path(path=local_path)

            progress_bar.update()
