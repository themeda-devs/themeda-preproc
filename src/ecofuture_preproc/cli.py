import argparse
import pathlib
import os

import ecofuture_preproc.roi
import ecofuture_preproc.source


def main() -> None:

    parser = argparse.ArgumentParser(
        description="Pre-processing for the Ecofuture project",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    default_base_output_dir = os.environ.get(
        "ECOFUTURE_PREPROC_BASE_OUTPUT_DIR",
        default=(
            pathlib.Path(__file__).parent /
            "../../../../" /
            "data"
        ).resolve(),
    )

    parser.add_argument(
        "-base_output_dir",
        type=pathlib.Path,
        required=False,
        default=default_base_output_dir,
        help="Base directory to write the output",
    )

    parser.add_argument(
        "--protect",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Make the output read-only",
    )

    parser.add_argument(
        "--show_progress",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Show a progress bar, if applicable",
    )

    subparsers = parser.add_subparsers(dest="command")

    # roi prep
    roi_parser = subparsers.add_parser(
        "roi_prep",
        help="Pre-process a ROI definition",
    )

    _ = subparsers.add_parser(
        "acquire",
        help="Acquire the necessary data",
    )

    _ = subparsers.add_parser(
        "prep",
        help="Prepare the raw data for pre-processing",
    )

    to_chips_parser = subparsers.add_parser(
        "to_chips",
        help="Convert the data to a chip representation",
    )

    to_chiplets_parser = subparsers.add_parser(
        "to_chiplets",
        help="Convert the data to a chiplet representation",
    )

    for parser_needing_roi_name in [roi_parser, to_chips_parser, to_chiplets_parser]:
        parser_needing_roi_name.add_argument(
            "-roi_name",
            required=True,
            choices=list(ecofuture_preproc.roi.ROIName),
            type=ecofuture_preproc.roi.ROIName,
        )

    for parser_needing_source_name in [to_chips_parser, to_chiplets_parser]:
        parser_needing_source_name.add_argument(
            "-source_name",
            required=True,
            choices=list(ecofuture_preproc.source.DataSourceName),
            type=ecofuture_preproc.source.DataSourceName,
        )

    args = parser.parse_args()

    runner_lut = {
        "roi_prep": ecofuture_preproc.roi.run_prep,
    }

    kwargs = {key: value for (key, value) in vars(args).items() if key != "command"}

    runner_lut[args.command](**kwargs)


if __name__ == "__main__":
    main()
