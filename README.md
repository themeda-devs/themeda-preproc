# Ecofuture pre-processing

[[_TOC_]]

## Usage

Most external interaction with this package will be related to the final stage of the pre-preprocessing: the chiplets.

### Chiplet data loading

For loading the chiplets for a given year and data source, the relevant function is `ecofuture_preproc.chiplets.load_chiplets`.
This returns a 3D memmapped numpy array, where the first dimension is the chiplet instance, the second dimension is the vertical spatial dimension, and the third dimension is the horizontal spatial dimension.
Because the variable is memmapped, there is no memory cost to loading the chiplets until individual items are accessed.

For example, the chiplets for the land use data source from 1996 can be loaded by something like:

```python
chiplets = ecofuture_preproc.chiplets.load_chiplets(
    source_name=ecofuture_preproc.source.DataSourceName("land_use"),
    year=1996,
    roi_name=ecofuture_preproc.roi.ROIName("savanna"),
    base_size_pix=160,
    pad_size_pix=32,
    base_output_dir=pathlib.Path("~/data").expanduser(),
)
```

### Chiplet metadata access

To access the chiplets of interest within the loaded chiplet data structure, we need to know the properties of each chiplet index.
That information is obtained by loading the appropriate metadata table, using the `ecofuture_preproc.chiplet_table.load_table` function.
For example:

```python
table = ecofuture_preproc.chiplet_table.load_table(
    roi_name=ecofuture_preproc.roi.ROIName("savanna"),
    base_output_dir=pathlib.Path("~/data").expanduser(),
    pad_size_pix=32,
)
```

This returns a Polars `DataFrame` tabular representation, in which each row corresponds to a chiplet instance in the data array (with the specific index captured in the `index` column).
By selecting and filtering within this table, the indices of interest can be obtained and then used to index into the chiplet data array.


## Approach

The pre-processing of the data associated with each data source occurs within four sequential stages: acquisition, preparation, chip conversion, and chiplet conversion.
Each data source handler has a directory in the package, containing files implementing each of these steps: `acquire.py`, `prep.py`, `to_chips.py`, and `to_chips.py`.
Note that Rain and Tmax data sources are both processed through a common `climate` handler, and the two fire scar data sources (early and late) are processed through a common `fire_scar` handler.

These four stages are supported by two additional steps:

### Region of Interest (ROI) preparation

This stage involves converting a ROI description from a GeoJSON file into a `shapely`-based representation with a Coordinate Reference System (CRS) of the Australian Albers (3577) projection.
The GeoJSON files are stored within the `resources/roi` directory of this package, and the prepared ROI files are written to the `data/roi` directory.

See `roi.py` in the package for details.

### Chiplet table preparation

This stage involves creating a tabular representation of the metadata for each of the 'chiplet' representations, which are the final form of the data that are used in subsequent analyses.
The set of chiplets, and hence the chiplet tables, depend on the ROI used and the padding size used in the chiplet formation.
The resulting tables are stored in a parquet file within `data/chiplet_table/roi_${ROI_NAME}/pad_${PAD_SIZE_PIX}/`.

See `chiplet_table.py` in the package for details.

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


### Chiplet conversion

The final stage involves creating a set of 'chiplets' for each year for each data source, where a chiplet is a small spatial region of interest - typically with a base size of 160 x 160 pixels.
The chiplets for a given data source are stored inside a single numpy array per year, saved in `data/chiplets/roi_${ROI_NAME}/pad_${PAD_SIZE_PIX}/${DATA_SOURCE}`.
The 'padding' refers to the region shared by chiplets with neighbouring spatial locations; rather than tiling the space without overlap, the boundary of each chiplet is extended by `${PAD_SIZE_PIX}` number of pixels.
A chiplet is considered valid if it has any spatial overlap with the named ROI, without considering any padded regions.

There is some data-source specificity to the chiplet conversion process:

* Land cover: The value of each pixel is relabelled from the Level 4 classification of Digital Earth Australia into a custom re-classification.
The lookup table for this re-labelling is found in the `resources/relabel` directory of this package.
Additionally, the pixels classified as 'water' can be re-labelled as 'ocean' of the pixel lies outside of a ROI (called 'australia') that has coastal boundaries.
* Land use: Similar to the land cover data, the land use classifications are re-labelled to a custom classification system.
* Rain, Tmax: Just a direct transformation to chiplets, with a conversion to 16-bit floating-point values.
* Elevation: Just a direct transformation to chiplets, with a conversion to 16-bit floating-point values.
* Fire scar early, fire scar late: Just a direct transformation to chiplets.
