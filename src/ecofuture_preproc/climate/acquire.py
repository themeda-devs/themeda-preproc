"""
Acquires the ANU climate data (rain, tmax) by downloading the NetCDF files for all
years and months.
"""

import pathlib
import contextlib

import siphon.catalog

import tqdm

import ecofuture_preproc.source
import ecofuture_preproc.utils


def run(
    source_name: ecofuture_preproc.source.DataSourceName,
    base_output_dir: pathlib.Path,
    protect: bool = True,
    show_progress: bool = True,
) -> None:

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

    for (year, year_data) in catalog.catalog_refs.items():

        if len(year) != 4 or not year.isnumeric():
            raise ValueError(f"Unexpected year ({year})")

        year_catalog = year_data.follow()

        for (dataset_name, dataset) in year_catalog.datasets.items():

            if not dataset.name.endswith(".nc"):
                raise ValueError(f"Unexpected dataset name ({dataset.name})")

            local_path = output_dir / dataset.name

            if not local_path.exists():
                to_download[dataset] = local_path

    with contextlib.closing(
        tqdm.tqdm(
            iterable=None,
            total=len(to_download),
            disable=not show_progress,
        )
    ) as progress_bar:

        for (dataset, local_path) in to_download.items():

            dataset.download(filename=str(local_path))

            if protect:
                ecofuture_preproc.utils.protect_path(path=local_path)

            progress_bar.update()
