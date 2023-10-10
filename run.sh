#!/usr/bin/env bash

# uncomment AFTER command finishes, not after launching

# ROI prep
#poetry run ecofuture_preproc roi_prep -roi_name savanna
#poetry run ecofuture_preproc roi_prep -roi_name australia

# DEA land cover
#poetry run ecofuture_preproc acquire -source_name land_cover
#poetry run ecofuture_preproc prep -source_name land_cover
#poetry run ecofuture_preproc to_chips -source_name land_cover -roi_name savanna

# chiplet table prep
#poetry run ecofuture_preproc chiplet_table_prep -roi_name savanna -pad_size_pix 32
#poetry run ecofuture_preproc chiplet_table_prep -roi_name savanna -pad_size_pix 0

# land cover
#poetry run ecofuture_preproc to_chiplets -source_name land_cover -roi_name savanna -pad_size_pix 32
#poetry run ecofuture_preproc to_chiplets -source_name land_cover -roi_name savanna -pad_size_pix 0
#poetry run ecofuture_preproc chiplets_to_geotiff -source_name land_cover -roi_name savanna
#xvfb-run poetry run ecofuture_preproc plot_maps -source_name land_cover -roi_name savanna

# ANU climate - tmax
#poetry run ecofuture_preproc acquire -source_name tmax
#poetry run ecofuture_preproc prep -source_name tmax
#poetry run ecofuture_preproc to_chips -source_name tmax -roi_name savanna
#poetry run ecofuture_preproc -cores 2 to_chiplets -source_name tmax -roi_name savanna -pad_size_pix 32
#poetry run ecofuture_preproc -cores 2 to_chiplets -source_name tmax -roi_name savanna -pad_size_pix 0
#poetry run ecofuture_preproc -cores 32 denan_chiplets -source_name tmax -roi_name savanna -pad_size_pix 32
#poetry run ecofuture_preproc -cores 32 denan_chiplets -source_name tmax -roi_name savanna -pad_size_pix 0
#poetry run ecofuture_preproc -cores 32 chiplets_to_geotiff -source_name tmax -roi_name savanna
#xvfb-run poetry run ecofuture_preproc plot_maps -source_name tmax -roi_name savanna
#poetry run ecofuture_preproc summary_stats -source_name tmax -roi_name savanna

# ANU climate - rain
#poetry run ecofuture_preproc acquire -source_name rain
#poetry run ecofuture_preproc prep -source_name rain
#poetry run ecofuture_preproc to_chips -source_name rain -roi_name savanna
#poetry run ecofuture_preproc -cores 2 to_chiplets -source_name rain -roi_name savanna -pad_size_pix 32
#poetry run ecofuture_preproc -cores 2 to_chiplets -source_name rain -roi_name savanna -pad_size_pix 0
#poetry run ecofuture_preproc -cores 32 denan_chiplets -source_name rain -roi_name savanna -pad_size_pix 32
#poetry run ecofuture_preproc -cores 32 denan_chiplets -source_name rain -roi_name savanna -pad_size_pix 0
#poetry run ecofuture_preproc -cores 32 chiplets_to_geotiff -source_name rain -roi_name savanna
#xvfb-run poetry run ecofuture_preproc plot_maps -source_name rain -roi_name savanna
#poetry run ecofuture_preproc summary_stats -source_name rain -roi_name savanna
#poetry run ecofuture_preproc summary_stats -source_name rain -roi_name savanna --log_transformed

# land use
#poetry run ecofuture_preproc acquire -source_name land_use
#poetry run ecofuture_preproc prep -source_name land_use
#poetry run ecofuture_preproc to_chips -source_name land_use -roi_name savanna
poetry run ecofuture_preproc to_chiplets -source_name land_use -roi_name savanna -pad_size_pix 32
#poetry run ecofuture_preproc to_chiplets -source_name land_use -roi_name savanna -pad_size_pix 0
#poetry run ecofuture_preproc -cores 32 chiplets_to_geotiff -source_name land_use -roi_name savanna
#xvfb-run poetry run ecofuture_preproc plot_maps -source_name land_use -roi_name savanna

# fire scar - early
#poetry run ecofuture_preproc prep -source_name fire_scar_early
#poetry run ecofuture_preproc to_chips -source_name fire_scar_early -roi_name savanna
#poetry run ecofuture_preproc to_chiplets -source_name fire_scar_early -roi_name savanna -pad_size_pix 32
#poetry run ecofuture_preproc to_chiplets -source_name fire_scar_early -roi_name savanna -pad_size_pix 0
#poetry run ecofuture_preproc -cores 32 chiplets_to_geotiff -source_name fire_scar_early -roi_name savanna
#xvfb-run poetry run ecofuture_preproc plot_maps -source_name fire_scar_early -roi_name savanna

# fire scar - late
#poetry run ecofuture_preproc prep -source_name fire_scar_late
#poetry run ecofuture_preproc to_chips -source_name fire_scar_late -roi_name savanna
#poetry run ecofuture_preproc to_chiplets -source_name fire_scar_late -roi_name savanna -pad_size_pix 32
#poetry run ecofuture_preproc to_chiplets -source_name fire_scar_late -roi_name savanna -pad_size_pix 0
#poetry run ecofuture_preproc -cores 32 chiplets_to_geotiff -source_name fire_scar_late -roi_name savanna
#xvfb-run poetry run ecofuture_preproc plot_maps -source_name fire_scar_late -roi_name savanna

# elevation
#poetry run ecofuture_preproc acquire -source_name elevation
#poetry run ecofuture_preproc prep -source_name elevation
#poetry run ecofuture_preproc to_chips -source_name elevation -roi_name savanna
#poetry run ecofuture_preproc -cores 2 to_chiplets -source_name elevation -roi_name savanna -pad_size_pix 32
#poetry run ecofuture_preproc -cores 2 to_chiplets -source_name elevation -roi_name savanna -pad_size_pix 0
#poetry run ecofuture_preproc -cores 32 denan_chiplets -source_name elevation -roi_name savanna -pad_size_pix 32
#poetry run ecofuture_preproc -cores 32 denan_chiplets -source_name elevation -roi_name savanna -pad_size_pix 0
#poetry run ecofuture_preproc -cores 32 chiplets_to_geotiff -source_name elevation -roi_name savanna
#xvfb-run poetry run ecofuture_preproc plot_maps -source_name elevation -roi_name savanna
#poetry run ecofuture_preproc summary_stats -source_name elevation -roi_name savanna

# soil depth
#poetry run ecofuture_preproc acquire -source_name soil_depth
#poetry run ecofuture_preproc prep -source_name soil_depth
#poetry run ecofuture_preproc to_chips -source_name soil_depth -roi_name savanna
#poetry run ecofuture_preproc -cores 2 to_chiplets -source_name soil_depth -roi_name savanna -pad_size_pix 32
#poetry run ecofuture_preproc -cores 2 to_chiplets -source_name soil_depth -roi_name savanna -pad_size_pix 0
#poetry run ecofuture_preproc -cores 32 denan_chiplets -source_name soil_depth -roi_name savanna -pad_size_pix 32
#poetry run ecofuture_preproc -cores 32 denan_chiplets -source_name soil_depth -roi_name savanna -pad_size_pix 0
#poetry run ecofuture_preproc -cores 32 chiplets_to_geotiff -source_name soil_depth -roi_name savanna
#xvfb-run poetry run ecofuture_preproc plot_maps -source_name soil_depth -roi_name savanna
#poetry run ecofuture_preproc summary_stats -source_name soil_depth -roi_name savanna

# soil clay
#poetry run ecofuture_preproc acquire -source_name soil_clay
#poetry run ecofuture_preproc prep -source_name soil_clay
#poetry run ecofuture_preproc to_chips -source_name soil_clay -roi_name savanna
#poetry run ecofuture_preproc -cores 2 to_chiplets -source_name soil_clay -roi_name savanna -pad_size_pix 32
#poetry run ecofuture_preproc -cores 2 to_chiplets -source_name soil_clay -roi_name savanna -pad_size_pix 0
#poetry run ecofuture_preproc -cores 32 denan_chiplets -source_name soil_clay -roi_name savanna -pad_size_pix 32
#poetry run ecofuture_preproc -cores 32 denan_chiplets -source_name soil_clay -roi_name savanna -pad_size_pix 0
#poetry run ecofuture_preproc -cores 32 chiplets_to_geotiff -source_name soil_clay -roi_name savanna
#xvfb-run poetry run ecofuture_preproc plot_maps -source_name soil_clay -roi_name savanna
#poetry run ecofuture_preproc summary_stats -source_name soil_clay -roi_name savanna

# soil ece
#poetry run ecofuture_preproc acquire -source_name soil_ece
#poetry run ecofuture_preproc prep -source_name soil_ece
#poetry run ecofuture_preproc to_chips -source_name soil_ece -roi_name savanna
#poetry run ecofuture_preproc -cores 2 to_chiplets -source_name soil_ece -roi_name savanna -pad_size_pix 32
#poetry run ecofuture_preproc -cores 2 to_chiplets -source_name soil_ece -roi_name savanna -pad_size_pix 0
#poetry run ecofuture_preproc -cores 32 denan_chiplets -source_name soil_ece -roi_name savanna -pad_size_pix 32
#poetry run ecofuture_preproc -cores 32 denan_chiplets -source_name soil_ece -roi_name savanna -pad_size_pix 0
#poetry run ecofuture_preproc -cores 32 chiplets_to_geotiff -source_name soil_ece -roi_name savanna
#xvfb-run poetry run ecofuture_preproc plot_maps -source_name soil_ece -roi_name savanna
#poetry run ecofuture_preproc summary_stats -source_name soil_ece -roi_name savanna
