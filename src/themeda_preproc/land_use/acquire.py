"""
Acquires the land use data by downloading the zipped data from the relevant websites.
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

    urls = [
        "https://anrdl-integration-web-catalog-saxfirxkxt.s3-ap-southeast-2.amazonaws.com/warehouse/luav4g9abl078/luav4g9abl07811a01egialb132.zip",
        "https://www.agriculture.gov.au/sites/default/files/documents/LanduseofAustralia_93-02v3.zip",
        "https://www.agriculture.gov.au/sites/default/files/documents/nlum_alumv8_250m_2010_11_alb.zip",
        "https://www.agriculture.gov.au/sites/default/files/documents/nlum_alumv8_250m_2015_16_alb.zip",
    ]

    for url in urls:
        (*_, filename) = url.split("/")

        local_path = output_dir / filename

        if not local_path.exists():
            themeda_preproc.utils.download_file(
                url=url,
                output_path=local_path,
            )

            if protect:
                themeda_preproc.utils.protect_path(path=local_path)
