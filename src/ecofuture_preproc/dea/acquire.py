import pathlib
import contextlib

import boto3
import botocore

import ecofuture_preproc.source


def run(
    source_name: ecofuture_preproc.source.DataSourceName,
    base_output_dir: pathlib.Path,
    protect: bool = True,
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

        for page in pages:
            for item in page["Contents"]:

                path = pathlib.Path(item["Key"])

                if path.name.endswith("level4.tif"):

                    local_path = output_dir / path.name

                    if not local_path.exists():
                        client.download_file(
                            Bucket=bucket,
                            Key=item["Key"],
                            Filename=str(local_path),
                        )

                    if protect:
                        ecofuture_preproc.utils.protect_path(path=local_path)
