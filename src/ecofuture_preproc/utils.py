import pathlib


def protect_file(
    path: pathlib.Path,
    permissions: int = 0o440,
) -> None:
    path.chmod(mode=permissions)
