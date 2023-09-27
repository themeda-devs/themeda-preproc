import pathlib
import types
import typing
import pickle

import fiona

import odc.geo.geom

import ecofuture_preproc.source
import ecofuture_preproc.utils
import ecofuture_preproc.land_cover.utils


def run(
    source_name: ecofuture_preproc.source.DataSourceName,
    base_output_dir: pathlib.Path,
    protect: bool = True,
) -> None:
    raw_dir = base_output_dir / "raw" / "fire_scar" / "Fire_scars_1989_2023"
    prep_dir = base_output_dir / "prep" / source_name.value

    raw_year_paths = types.MappingProxyType(
        {get_year_from_path(path=path): path for path in sorted(raw_dir.glob("*.shp"))}
    )

    for year, raw_year_path in raw_year_paths.items():
        # 2023 and beyond will be incomplete, at this point
        if year >= 2023:
            continue

        output_dir = prep_dir / str(year)
        output_dir.mkdir(exist_ok=True, parents=True)

        output_path = output_dir / f"{source_name.value}_{year}.pkl"

        if not ecofuture_preproc.utils.is_path_existing_and_read_only(path=output_path):
            year_data = process_year(
                source_name=source_name,
                path=raw_year_path,
            )

            with output_path.open("wb") as handle:
                pickle.dump(year_data, handle)

        if protect:
            ecofuture_preproc.utils.protect_path(path=output_path)


def get_year_from_path(path: pathlib.Path) -> int:
    if not path.suffix == ".shp":
        raise ValueError("Expected suffix to be .shp")

    file_stem = path.stem

    if not file_stem.startswith("JW_aust"):
        raise ValueError("Unexpected path format")

    year_f = float(file_stem.strip("JW_aust")[:4])

    if not year_f.is_integer():
        raise ValueError("Unexpected year")

    year = int(year_f)

    return year


def process_year(
    source_name: ecofuture_preproc.source.DataSourceName,
    path: pathlib.Path,
) -> list[odc.geo.geom.Geometry]:
    geoms = []

    with fiona.open(fp=path) as handle:
        for entry in handle:
            season = get_season_of_entry(entry=entry)

            if not source_name.value.endswith(season):
                continue
            else:
                if not source_name.value.endswith(season):
                    raise ValueError("Unexpected season")

                geom = geom_from_shape(
                    entry=entry,
                    src_crs=handle.crs,
                )

                geoms.append(geom)

    return geoms


def get_season_of_entry(entry: fiona.model.Feature) -> str:
    if "DATE" in entry.properties:
        date_str = entry.properties["DATE"]

        if len(date_str) != 8:
            raise ValueError(f"Unexpected date string: {date_str}")

        # YYYYMMDD format
        month = int(date_str[4:6])

    elif "DATE1" in entry.properties:
        date_str = entry.properties["DATE1"]

        (_, month, _) = date_str.split("-")

        month = int(month)

    else:
        raise ValueError("Cannot find date; {entry.properties.__dict__}")

    if not (1 <= month <= 12):
        raise ValueError(f"Unexpected month: {month}")

    if month <= 7:
        season = "early"
    else:
        season = "late"

    return season


def geom_from_shape(
    entry: fiona.model.Feature,
    src_crs: fiona.crs.CRS,
    dst_crs: typing.Optional[int] = 3577,
) -> odc.geo.geom.Geometry:
    if entry.geometry.type == "Polygon":
        (outer, *inners) = entry.geometry.coordinates
        geom = odc.geo.geom.polygon(
            outer,
            src_crs,
            *inners,
        )

    elif entry.geometry.type == "MultiPolygon":
        geom = odc.geo.geom.multipolygon(
            coords=entry.geometry.coordinates,
            crs=src_crs,
        )
    else:
        raise ValueError("Unexpected geometry type")

    if dst_crs is not None:
        geom = geom.to_crs(crs=dst_crs)

    return geom
