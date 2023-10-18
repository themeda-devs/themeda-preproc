"""
Acquires the land use data by downloading the zipped data from the relevant websites.

Needs to be unzipped manually after this (in the same directory), to give:
srtm-1sec-demh-v1-COG.tif
"""

import pathlib

import themeda_preproc.source
import themeda_preproc.utils


def run(
    source_name: themeda_preproc.source.DataSourceName,
    base_output_dir: pathlib.Path,
    protect: bool = True,
) -> None:
    output_dir = base_output_dir / "raw" / source_name.value

    url = "/".join(
        [
            "https://elevation-direct-downloads.s3-ap-southeast-2.amazonaws.com",
            "1sec-dem",
            "71498.zip",
        ]
    )

    (*_, filename) = url.split("/")

    local_path = output_dir / filename

    if not local_path.exists():
        themeda_preproc.utils.download_file(
            url=url,
            output_path=local_path,
        )

    if protect:
        themeda_preproc.utils.protect_path(path=local_path)
