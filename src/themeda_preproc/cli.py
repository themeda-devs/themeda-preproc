import argparse
import pathlib
import os
import importlib
import inspect

import numpy as np

import themeda_preproc.roi
import themeda_preproc.source


def main() -> None:
    # silence the dask chunks message
    # array.slicing.split_large_chunks
    os.environ["DASK_" + "ARRAY__" + "SLICING__" + "SPLIT_LARGE_CHUNKS"] = "false"

    parser = argparse.ArgumentParser(
        description="Pre-processing for the Themeda project",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    default_base_output_dir = os.environ.get(
        "THEMEDA_PREPROC_BASE_OUTPUT_DIR",
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

    denan_chiplets_parser = subparsers.add_parser(
        "denan_chiplets",
        help="Replace the NaNs in the chiplets for continuous data sources",
    )

    stats_parser = subparsers.add_parser(
        "summary_stats",
        help="Calculate summary stats for the continuous data sources",
    )

    chiplets_to_geotiff_parser = subparsers.add_parser(
        "chiplets_to_geotiff",
        help="Convert the chiplet array data to GeoTIFF representations",
    )

    plot_maps_parser = subparsers.add_parser(
        "plot_maps",
        help="Plot a map of the data source for each year",
    )

    transect_parser = subparsers.add_parser(
        "transect",
        help="Extract data along the NATT",
    )

    form_hash_db_parser = subparsers.add_parser(
        "form_hash_db",
        help="Calculate the hashes of all files in the output directory",
    )

    check_against_hash_db_parser = subparsers.add_parser(
        "check_against_hash_db",
        help="Compares the hashes of all files in the output directory",
    )

    pad_chiplets_parser = subparsers.add_parser(
        "pad_chiplets",
        help="Converts chiplets without padding to have padding",
    )

    for parser_needing_roi_name in [
        roi_parser,
        to_chips_parser,
        to_chiplets_parser,
        denan_chiplets_parser,
        chiplet_table_parser,
        chiplets_to_geotiff_parser,
        stats_parser,
        plot_maps_parser,
        transect_parser,
        pad_chiplets_parser,
    ]:
        parser_needing_roi_name.add_argument(
            "-roi_name",
            required=True,
            choices=list(themeda_preproc.roi.ROIName),
            type=themeda_preproc.roi.ROIName,
        )

    for parser_needing_source_name in [
        acquire_parser,
        prep_parser,
        to_chips_parser,
        to_chiplets_parser,
        denan_chiplets_parser,
        chiplets_to_geotiff_parser,
        plot_maps_parser,
        transect_parser,
        stats_parser,
    ]:
        parser_needing_source_name.add_argument(
            "-source_name",
            required=True,
            choices=list(themeda_preproc.source.DataSourceName),
            type=themeda_preproc.source.DataSourceName,
        )

    for parser_needing_base_size_pix in [
        chiplet_table_parser,
        to_chiplets_parser,
        chiplets_to_geotiff_parser,
        pad_chiplets_parser,
    ]:
        parser_needing_base_size_pix.add_argument(
            "-base_size_pix",
            type=int,
            default=160,
        )

    for parser_needing_pad_size_pix in [
        chiplet_table_parser,
        to_chiplets_parser,
        denan_chiplets_parser,
        chiplets_to_geotiff_parser,
        pad_chiplets_parser,
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

    for parser_needing_log_transformed in [stats_parser]:
        parser_needing_log_transformed.add_argument(
            "--log_transformed",
            action=argparse.BooleanOptionalAction,
            default=False,
        )

    for parser_needing_hash_db_path in [
        form_hash_db_parser,
        check_against_hash_db_parser,
    ]:
        parser_needing_hash_db_path.add_argument(
            "-hash_db_path",
            required=True,
            type=pathlib.Path,
        )

    for parser_needing_output_path in [
        pad_chiplets_parser,
    ]:
        parser_needing_output_path.add_argument(
            "-output_path",
            required=True,
            type=pathlib.Path,
        )

    for parser_needing_chiplets_path in [
        pad_chiplets_parser,
    ]:
        parser_needing_chiplets_path.add_argument(
            "-chiplets_path",
            required=True,
            type=pathlib.Path,
        )

    for parser_needing_dtype in [
        pad_chiplets_parser,
    ]:
        parser_needing_dtype.add_argument(
            "-dtype",
            required=True,
            type=np.dtype,
            help="Chiplet datatype (e.g., 'uint8')",
        )

    for parser_needing_n_in_extra_dim in [
        pad_chiplets_parser,
    ]:
        parser_needing_n_in_extra_dim.add_argument(
            "-n_in_extra_dim",
            required=False,
            type=int,
            help="Number of values in an extra second axis (or 0 if absent)",
            default=0,
        )

    args = parser.parse_args()

    if args.command is None:
        raise ValueError("Please provide a command")

    if args.command == "roi_prep":
        runner_str = "themeda_preproc.roi"
    elif args.command == "chiplet_table_prep":
        runner_str = "themeda_preproc.chiplet_table"
    elif args.command == "denan_chiplets":
        runner_str = "themeda_preproc.denan_chiplets"
    elif args.command == "chiplets_to_geotiff":
        runner_str = "themeda_preproc.chiplet_geotiff"
    elif args.command == "transect":
        runner_str = "themeda_preproc.transect"
    elif args.command == "summary_stats":
        runner_str = "themeda_preproc.summary_stats"
    elif args.command == "form_hash_db":
        runner_str = "themeda_preproc.hashcheck"
        runner_function = "run_form_hash_db"
    elif args.command == "check_against_hash_db":
        runner_str = "themeda_preproc.hashcheck"
        runner_function = "run_check_against_hash_db"
    elif args.command == "pad_chiplets":
        runner_str = "themeda_preproc.pad_chiplets"
    else:
        handler_name = themeda_preproc.source.DATA_SOURCE_HANDLER[args.source_name]
        runner_str = f"themeda_preproc.{handler_name}.{args.command}"

    module = importlib.import_module(name=runner_str)
    try:
        function = getattr(module, runner_function)
    except NameError:
        function = module.run
    signature = inspect.signature(function)
    param_names = list(signature.parameters)
    args = {param_name: getattr(args, param_name) for param_name in param_names}

    function(**args)


if __name__ == "__main__":
    main()
