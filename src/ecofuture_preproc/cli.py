import argparse
import pathlib
import os
import importlib
import inspect

import ecofuture_preproc.roi
import ecofuture_preproc.source


def main() -> None:
    # silence the dask chunks message
    # array.slicing.split_large_chunks
    os.environ["DASK_" + "ARRAY__" + "SLICING__" + "SPLIT_LARGE_CHUNKS"] = "false"

    parser = argparse.ArgumentParser(
        description="Pre-processing for the Ecofuture project",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    default_base_output_dir = os.environ.get(
        "ECOFUTURE_PREPROC_BASE_OUTPUT_DIR",
        default=(pathlib.Path(__file__).parent / "../../../../" / "data").resolve(),
    )

    parser.add_argument(
        "-base_output_dir",
        type=pathlib.Path,
        required=False,
        default=default_base_output_dir,
        help="Base directory to write the output",
    )

    parser.add_argument(
        "-cores",
        type=int,
        required=False,
        default=4,
        help="Number of cores to use with multiprocessing",
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

    roi_parser = subparsers.add_parser(
        "roi_prep",
        help="Pre-process a ROI definition",
    )

    chiplet_table_parser = subparsers.add_parser(
        "chiplet_table_prep",
        help="Prepare the chiplet info table",
    )

    acquire_parser = subparsers.add_parser(
        "acquire",
        help="Acquire the necessary data",
    )

    prep_parser = subparsers.add_parser(
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

    chiplets_to_geotiff_parser = subparsers.add_parser(
        "chiplets_to_geotiff",
        help="Convert the chiplet array data to GeoTIFF representations",
    )

    plot_maps_parser = subparsers.add_parser(
        "plot_maps",
        help="Plot a map of the data source for each year",
    )

    for parser_needing_roi_name in [
        roi_parser,
        to_chips_parser,
        to_chiplets_parser,
        chiplet_table_parser,
        chiplets_to_geotiff_parser,
        plot_maps_parser,
    ]:
        parser_needing_roi_name.add_argument(
            "-roi_name",
            required=True,
            choices=list(ecofuture_preproc.roi.ROIName),
            type=ecofuture_preproc.roi.ROIName,
        )

    for parser_needing_source_name in [
        acquire_parser,
        prep_parser,
        to_chips_parser,
        to_chiplets_parser,
        chiplets_to_geotiff_parser,
        plot_maps_parser,
    ]:
        parser_needing_source_name.add_argument(
            "-source_name",
            required=True,
            choices=list(ecofuture_preproc.source.DataSourceName),
            type=ecofuture_preproc.source.DataSourceName,
        )

    for parser_needing_base_size_pix in [
        chiplet_table_parser,
        to_chiplets_parser,
        chiplets_to_geotiff_parser,
    ]:
        parser_needing_base_size_pix.add_argument(
            "-base_size_pix",
            type=int,
            default=160,
        )

    for parser_needing_pad_size_pix in [
        chiplet_table_parser,
        to_chiplets_parser,
        chiplets_to_geotiff_parser,
    ]:
        parser_needing_pad_size_pix.add_argument(
            "-pad_size_pix",
            type=int,
            default=32,
        )

    for parser_needing_resolution in [plot_maps_parser]:
        parser_needing_resolution.add_argument(
            "-resolution",
            type=float,
            default=1000.0,
        )

    for parser_needing_headless in [plot_maps_parser]:
        parser_needing_headless.add_argument(
            "--headless",
            action=argparse.BooleanOptionalAction,
            default=True,
        )

    args = parser.parse_args()

    if args.command is None:
        raise ValueError("Please provide a command")

    if args.command == "roi_prep":
        runner_str = "ecofuture_preproc.roi"
    elif args.command == "chiplet_table_prep":
        runner_str = "ecofuture_preproc.chiplet_table"
    elif args.command == "chiplets_to_geotiff":
        runner_str = "ecofuture_preproc.chiplet_geotiff"
    else:
        handler_name = ecofuture_preproc.source.DATA_SOURCE_HANDLER[args.source_name]
        runner_str = f"ecofuture_preproc.{handler_name}.{args.command}"

    module = importlib.import_module(name=runner_str)
    function = module.run
    signature = inspect.signature(function)
    param_names = list(signature.parameters)
    args = {param_name: getattr(args, param_name) for param_name in param_names}

    function(**args)


if __name__ == "__main__":
    main()
