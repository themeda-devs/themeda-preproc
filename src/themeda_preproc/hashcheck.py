import pathlib
import subprocess
import json

import tqdm

import themeda_preproc.utils



def run_form_hash_db(
    base_output_dir: pathlib.Path,
    hash_db_path: pathlib.Path,
    protect: bool,
    show_progress: bool = True,
) -> None:

    if themeda_preproc.utils.is_path_existing_and_read_only(path=hash_db_path):
        print(f"Hash DB at {hash_db_path} exists; skipping.")

    file_paths = get_all_file_paths_under_dir(base_dir=base_output_dir)

    hash_db: dict[str, str] = dict(
        tqdm.tqdm(
            (
                (
                    str(path.relative_to(base_output_dir)),
                    get_hash(path=path),
                )
                for path in file_paths
            ),
            total=len(file_paths),
            disable=not show_progress,
        )
    )

    with hash_db_path.open("w") as handle:
        json.dump(hash_db, handle, indent=1)

    if protect:
        themeda_preproc.utils.protect_path(path=hash_db_path)


def get_all_file_paths_under_dir(base_dir: pathlib.Path) -> list[pathlib.Path]:

    file_paths = sorted(
        [
            path
            for path in base_dir.rglob("*")
            if path.is_file()
        ]
    )

    return file_paths


def load_hash_db(hash_db_path: pathlib.Path) -> dict[str, str]:

    with hash_db_path.open("r") as handle:
        hash_db: dict[str, str] = json.load(handle)

    return hash_db


def get_hash(path: pathlib.Path) -> str:

    cmd_out = subprocess.check_output(["md5sum", str(path)], encoding="utf-8")

    # "The default mode is to print a line with checksum, a space, a character
    # indicating input mode ('*' for binary, ' ' for text or where binary is
    # insignificant), and name for each FILE."
    (path_hash, *_) = cmd_out.split(" ")

    return path_hash
