# Ecofuture pre-processing

[[_TOC_]]

## Installation

This package requires Python 3.9+ and Poetry.
First, clone the repository to a local directory:
```bash
git clone https://gitlab.unimelb.edu.au/mdap/ecofuture-preproc
```
Then change into the package directory and setup Poetry to use a new virtual environment:
```bash
poetry env use $(which python)
```
Then install the dependencies (optionally include the `--with=dev,interactive` argument if involved in package development):
```bash
poetry install
```

### Visualisation support

Unfortunately, it is a bit trickier to install the package with support for creating the visualisations and requires a bit of manual installation.

First, install the listed `vis` dependencies:
```bash
poetry install --with=vis
```

Then install the necessary system packages (see the [requirements for building `veusz` from source](https://github.com/veusz/veusz/blob/master/INSTALL.md#installing-from-source)) - the details of which depend on your operating system.

Then create a build directory, download the `veusz` source code, and extract:
```bash
# assuming in `code/ecofuture-preproc`
cd ../..
mkdir build
cd build
wget https://github.com/veusz/veusz/archive/refs/tags/veusz-3.6.2.zip
unzip veusz-3.6.2.zip
cd veusz-veusz-3.6.2
```
and then build and install:
```bash
poetry -C ../../code/ecofuture-preproc run pip install .
```

## Using the pre-processing output

Most external interaction with this package will be related to the final stage of the pre-preprocessing: the chiplets.
Note that most functions for interacting with this data ask for a `base_output_dir` argument; this is the root directory of all the data, that contains directories like `chiplets`, `chiplet_table`, `chips`, etc.

### Chiplet data loading

For loading the chiplets for a given year and data source, the relevant function is most likely `ecofuture_preproc.chiplets.chiplets_reader`.
This returns a context manager for a 3D memmapped numpy array, within which the first dimension is the chiplet instance, the second dimension is the vertical spatial dimension, and the third dimension is the horizontal spatial dimension.
Because the variable is memmapped, there is no memory cost to loading the chiplets until individual items are accessed.
The context manager closes the file handle upon exiting, so make sure to not have any lingering references to the chiplet data or else you may see segfaults.

For example, the chiplets for the land use data source from 1996 can be loaded by something like:

```python
with ecofuture_preproc.chiplets.chiplets_reader(
    source_name=ecofuture_preproc.source.DataSourceName("land_use"),
    year=1996,
    roi_name=ecofuture_preproc.roi.ROIName("savanna"),
    pad_size_pix=32,
    base_output_dir=pathlib.Path("~/data").expanduser(),
) as chiplets:
    # do stuff
```

There is also the lower-level `ecofuture_preproc.chiplets.load_chiplets` function, which does not clean up the memmap structure.
It includes the option to `load_into_ram`, if you want to access all the chiplet data and you have enough RAM.

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


### Chiplet summary statistics

For the continuous data sources, the mean, standard deviation, minimum, and maximum values have been computed across chiplets and years.
These values can be loaded using `ecofuture_preproc.summary_statistics.load_stats`.
For example:

```python
stats = ecofuture_preproc.summary_statistics.load_stats(
    source_name=source_name,
    roi_name=roi_name,
    base_output_dir=base_output_dir,
)
```

## Running the pre-processing

The pre-processing steps are executed via the `ecofuture_preproc` command; for example:
```bash
poetry run ecofuture_preproc --help
```

To see all the steps required to run through the complete pipeline, see `run.sh` in the root directory of the package.

> **Warning**
The pre-processing operations can consume a lot of RAM, CPU, and storage resources.
You can use the `-cores` option to `ecofuture_preproc` to limit the use of multiprocessing in the chiplets conversion stages.
Note that progress bars don't work all that well under multiprocessing (see [this issue with `tqdm`](https://github.com/tqdm/tqdm/issues/1000)); the only workaround I have found is to clear each progress bar after it has finished.
For unmonitored execution, you can use `--no-show_progress` to hide the progress bars.

### Approach

The pre-processing of the data associated with each data source occurs within a set of sequential stages: acquisition, preparation, chip conversion, chiplet conversion, conversion from chiplets to GeoTIFF files, map-based visualisations, and extracting data along the Northern Australia Tropical Transect (NATT).
Each data source handler has a directory in the package, containing files implementing these steps: `acquire.py`, `prep.py`, `to_chips.py`, `to_chiplets.py`, and `plot_maps.py` (the conversion from chiplets to GeoTIFF files, the de-NaN processing, the summary statistics, and the NATT extraction are the same across data sources and so are handled by `chiplet_geotiff.py`, `denan_chiplets.py`, `summary_statistics.py`, and `transect.py`, respectively).
The steps are executed by passing the appropriate step name as a positional argument to `ecofuture_preproc`.

> **Note**
Rain and Tmax data sources are both processed through a common `climate` handler, the two fire scar data sources (early and late) are processed through a common `fire_scar` handler, and the three soil data sources are processed through a common `soil` handler.

These stages are supported by two additional steps:

#### Region of Interest (ROI) preparation

This stage involves converting a ROI description from a GeoJSON file into a `shapely`-based representation with a Coordinate Reference System (CRS) of the Australian Albers (3577) projection.
The GeoJSON files are stored within the `resources/roi` directory of this package, and the prepared ROI files are written to the `data/roi` directory.

See `roi.py` in the package for details.

An example execution:
```bash
poetry run ecofuture_preproc roi_prep -roi_name savanna
```

#### Chiplet table preparation

This stage involves creating a tabular representation of the metadata for each of the 'chiplet' representations, which are the final form of the data that are used in subsequent analyses.
The set of chiplets, and hence the chiplet tables, depend on the ROI used and the padding size used in the chiplet formation.
The resulting tables are stored in a parquet file within `data/chiplet_table/roi_${ROI_NAME}/pad_${PAD_SIZE_PIX}/`.

See `chiplet_table.py` in the package for details.

An example execution:
```bash
poetry run ecofuture_preproc chiplet_table_prep -roi_name savanna -pad_size_pix 32
```
#### Acquisition

This stage relates to the `data/raw/${DATASOURCE}` directory.
For most of the data sources, it involves downloading raw data over HTTPS or S3 protocols.
There is typically little selection of data and files are kept in their native format and resolution as much as possible.

There are three exceptions:

1. The fire scar data is licensed and cannot be obtained through publicly-available storage.
Its acquisition stage requires manual copying of the data into the relevant directory.
1. Most of the years from the land use data source are, while publicly available, in a proprietary format (ArcGIS).
The ArcCatalog application is used to manually export the raster data into GeoTIFF format into the relevant directory.
The attribute tables are also exported via ArcCatalog, as plain text files.
1. The soil data is publicly available but without readily-utilised APIs, so they are manually downloaded using the links specified in the `acquire.py` file.

An example execution:
```bash
poetry run ecofuture_preproc acquire -source_name rain
```

#### Preparation

In this stage, the raw data for each data source are 'prepared' and placed into the `data/prep/${DATA_SOURCE}/${YYYY}` directory.
What is involved in 'preparation' varies across data sources, but the main idea is to minimally convert from the raw data into a representation suitable for project-specific analysis.

* Land cover: Chips are copied into `prep` if their spatial locations have data from all the possible years for the data source.
* Land use: For the chips that required conversion into GeoTIFF format, the pixel values are re-labelled so that they reflect the appropriate `LU_CODE` entry in their attribute table.
* Rain, Tmax: The raw monthly data is summarised into a single value per year, based on the mean (Tmax) or sum (Rain) operation, and years containing less than 12 months of data are culled.
* Elevation: The chip is simply assigned a year (2011) and copied over.
* Fire scar early, fire scar late: The raw shape files are split based on their season (before or after the end of June), and the shape files are converted into geometry objects and saved in pickled format.
* Soil: The chips are simply assigned a year (clay: 2021, depth: 2019, ece: 2014) and copied over.

An example execution:
```bash
poetry run ecofuture_preproc prep -source_name land_cover
```

#### Chip conversion

This involves converting each data source into a common spatial representation (coordinate system, resolution, and coverage), with the land cover chips as the canonical coordinate system and resolution and a named region of interest (ROI) as the spatial coverage.
As the canonical source, the land cover chips need to be converted from the preparation stage before the other data sources.

* Land cover: Chips are written to the `data/chips/roi_${ROI_NAME}/land_cover` directory if the spatial coverage of the chip has *any* overlap with the nominated ROI.
* Land use: The land use chip for a given year is resampled into the space of each land cover chip (using nearest-neighbour interpolation).
* Rain, Tmax: The chip for a given year is resampled into the space of each land cover chip (using bilinear interpolation).
* Elevation: The sole chip for this data source is resampled into the space of each land cover chip (using bilinear interpolation).
* Fire scar early, fire scar late: Within a year and land cover chip, the shapes for each fire scar instance are first tested for intersection with the spatial coverage of the land cover chip.
Those fire scar instance shapes that intersect with the land cover chip are then rasterised based on the spatial properties of the chip, and then aggregated (summed) over fire scar instances.
* Soil: The chips for this data source is resampled into the space of each land cover chip (using bilinear interpolation).

An example execution:
```bash
poetry run ecofuture_preproc to_chips -source_name tmax -roi_name savanna
```

#### Chiplet conversion

This stage involves creating a set of 'chiplets' for each year for each data source, where a chiplet is a small spatial region of interest - typically with a base size of 160 x 160 pixels.
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
* Soil: Just a direct transformation to chiplets, with a conversion to 16-bit floating-point values.

An example execution:
```bash
poetry run ecofuture_preproc to_chiplets -source_name fire_scar_early -roi_name savanna -pad_size_pix 32
```

#### Replacing NaNs

This stage replaces any NaNs that are present in the chiplet data for sources with continuous values, typically withe mean of the other values in the chiplet.

An example execution:
```bash
poetry run ecofuture_preproc denan_chiplets -source_name elevation -roi_name savanna -pad_size_pix 32
```

#### Summary statistics

This stage computes the mean and standard deviation for all values across space and years for each of the continuous data sources.

An example execution:
```bash
poetry run ecofuture_preproc summary_statistics -source_name soil_depth -roi_name savanna
```

#### Conversion of chiplets to GeoTIFF gridded chips

Although the above representation of the chiplets is best suited for loading into a neural network model, it is easier to otherwise interrogate the chiplet data if it is stored in GeoTIFF format.
Hence, this step forms GeoTIFF files for each of the grid references in the common chip space (i.e., the output of the `to_chips` step).
Note that this assumes that chiplets with a padding size of 0 have been created.

An example execution:
```bash
poetry run ecofuture_preproc chiplets_to_geotiff -source_name land_cover -roi_name savanna
```

#### Creating map visualisations

The GeoTIFF files created in the previous step can be used to create visualisations where the data are represented in a map form, with a separate page for each year in the data source.

An example execution:
```bash
poetry run ecofuture_preproc plot_maps -source_name soil_depth -roi_name savanna
```

#### Transect extraction

The GeoTIFF files are used to extract data along the coordinates of the NATT for all years of a given data source.

An example execution:
```bash
poetry run ecofuture_preproc transect -source_name soil_ece -roi_name savanna
```

## Authors

Staff at the [Melbourne Data Analytics Platform (MDAP)](https://mdap.unimelb.edu.au/), University of Melbourne.
