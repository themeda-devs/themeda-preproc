"""
Acquires the land cover data by downloading the GeoTIFF files for all years and
spatial regions from the DEA database.
"""

import pathlib
import contextlib

import boto3
import botocore

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

    bucket = "dea-public-data"
    product = "ga_ls_landcover_class_cyear_2"

    with contextlib.closing(
        boto3.client(
            "s3",
            config=botocore.client.Config(signature_version=botocore.UNSIGNED),
        )
    ) as client:
        paginator = client.get_paginator("list_objects")

        pages = paginator.paginate(
            Bucket=bucket,
            Prefix=f"derivative/{product}/1-0-0/",
        )

        to_download = {}

        for page in pages:
            for item in page["Contents"]:
                path = pathlib.Path(item["Key"])

                if path.name.endswith("level4.tif"):
                    local_path = output_dir / path.name

                    if not ecofuture_preproc.utils.is_path_existing_and_read_only(
                        path=local_path
                    ):
                        to_download[item["Key"]] = local_path

        with contextlib.closing(
            tqdm.tqdm(
                iterable=None,
                total=len(to_download),
                disable=not show_progress,
            )
        ) as progress_bar:
            for file_key, local_path in to_download.items():
                client.download_file(
                    Bucket=bucket,
                    Key=file_key,
                    Filename=str(local_path),
                )

                if protect:
                    ecofuture_preproc.utils.protect_path(path=local_path)

                progress_bar.update()
