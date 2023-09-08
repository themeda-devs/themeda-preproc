# Ecofuture pre-processing

## Approach

The pre-processing of the data associated with each data source occurs within four sequential stages.

These four stages are supported by two additional steps:

### Region of Interest (ROI) preparation

This stage involves converting a ROI description from a GeoJSON file into a `shapely`-based representation with a Coordinate Reference System (CRS) of the Australian Albers (3577) projection.
The GeoJSON files are stored within the `resources/roi` directory of this package, and the prepared ROI files are written to the `data/roi` directory.

### Chiplet table preparation

This stage involves creating a tabular representation of the metadata for each of the 'chiplet' representations, which are the final form of the data that are used in subsequent analyses.
The set of chiplets, and hence the chiplet tables, depend on the ROI used and the padding size used in the chiplet formation.
The resulting tables are stored in a parquet file within `data/chiplet_table/roi_${ROI_NAME}/pad_${PAD_SIZE_PIX}/`.

### Acquisition

This stage relates to the `data/raw/${DATASOURCE}` directory.
For most of the data sources, it involves downloading raw data over HTTPS or S3 protocols.
There is typically little selection of data and files are kept in their native format and resolution as much as possible.

There are two exceptions:

1. The fire scar data is licensed and cannot be obtained through publicly-available storage.
Its acquisition stage requires manual copying of the data into the relevant directory.
1. Most of the years from the land use data source are, while publicly available, in a proprietary format (ArcGIS).
The ArcCatalog application is used to manually export the raster data into GeoTIFF format into the relevant directory.
The attribute tables are also exported via ArcCatalog, as plain text files.

### Preparation

In this stage, the raw data for each data source are 'prepared' and placed into the `data/prep/${DATA_SOURCE}/${YYYY}` directory.
What is involved in 'preparation' varies across data sources, but the main idea is to minimally convert from the raw data into a representation suitable for project-specific analysis.

* Land cover: Chips are copied into `prep` if their spatial locations have data from all the possible years for the data source.
* Land use: For the chips that required conversion into GeoTIFF format, the pixel values are re-labelled so that they reflect the appropriate `LU_CODE` entry in their attribute table.
* Rain, Tmax: The raw monthly data is summarised into a single value per year, based on the mean (Tmax) or sum (Rain) operation, and years containing less than 12 months of data are culled.
* Elevation: The chip is simply assigned a year (2011) and copied over.
* Fire scar early, fire scar late: The raw shape files are split based on their season (before or after the end of June), and the shape files are converted into geometry objects and saved in pickled format.


### Chip conversion

This involves converting each data source into a common spatial representation (coordinate system, resolution, and coverage), with the land cover chips as the canonical coordinate system and resolution and a named region of interest (ROI) as the spatial coverage.
As the canonical source, the land cover chips need to be converted from the preparation stage before the other data sources.

* Land cover: Chips are written to the `data/chips/roi_${ROI_NAME}/land_cover` directory if the spatial coverage of the chip has *any* overlap with the nominated ROI.
* Land use: The land use chip for a given year is resampled into the space of each land cover chip (using nearest-neighbour interpolation).
* Rain, Tmax: The chip for a given year is resampled into the space of each land cover chip (using bilinear interpolation).
* Elevation: The sole chip for this data source is resampled into the space of each land cover chip (using bilinear interpolation).
* Fire scar early, fire scar late: Within a year and land cover chip, the shapes for each fire scar instance are first tested for intersection with the spatial coverage of the land cover chip.
Those fire scar instance shapes that intersect with the land cover chip are then rasterised based on the spatial properties of the chip, and then aggregated (summed) over fire scar instances.


