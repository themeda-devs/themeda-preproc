#!/usr/bin/env bash

# uncomment AFTER command finishes, not after launching

# ROI prep
#poetry run themeda_preproc roi_prep -roi_name savanna
#poetry run themeda_preproc roi_prep -roi_name australia

# DEA land cover
#poetry run themeda_preproc acquire -source_name land_cover
#poetry run themeda_preproc prep -source_name land_cover
#poetry run themeda_preproc to_chips -source_name land_cover -roi_name savanna

# chiplet table prep
#poetry run themeda_preproc chiplet_table_prep -roi_name savanna -pad_size_pix 32
#poetry run themeda_preproc chiplet_table_prep -roi_name savanna -pad_size_pix 0

# land cover
#poetry run themeda_preproc to_chiplets -source_name land_cover -roi_name savanna -pad_size_pix 32
#poetry run themeda_preproc to_chiplets -source_name land_cover -roi_name savanna -pad_size_pix 0
#poetry run themeda_preproc chiplets_to_geotiff -source_name land_cover -roi_name savanna
#xvfb-run poetry run themeda_preproc plot_maps -source_name land_cover -roi_name savanna

# ANU climate - tmax
#poetry run themeda_preproc acquire -source_name tmax
#poetry run themeda_preproc prep -source_name tmax
#poetry run themeda_preproc to_chips -source_name tmax -roi_name savanna
#poetry run themeda_preproc -cores 2 to_chiplets -source_name tmax -roi_name savanna -pad_size_pix 32
#poetry run themeda_preproc -cores 2 to_chiplets -source_name tmax -roi_name savanna -pad_size_pix 0
#poetry run themeda_preproc -cores 32 denan_chiplets -source_name tmax -roi_name savanna -pad_size_pix 32
#poetry run themeda_preproc -cores 32 denan_chiplets -source_name tmax -roi_name savanna -pad_size_pix 0
#poetry run themeda_preproc -cores 32 chiplets_to_geotiff -source_name tmax -roi_name savanna
#xvfb-run poetry run themeda_preproc plot_maps -source_name tmax -roi_name savanna
#poetry run themeda_preproc summary_stats -source_name tmax -roi_name savanna

# ANU climate - rain
#poetry run themeda_preproc acquire -source_name rain
#poetry run themeda_preproc prep -source_name rain
#poetry run themeda_preproc to_chips -source_name rain -roi_name savanna
#poetry run themeda_preproc -cores 2 to_chiplets -source_name rain -roi_name savanna -pad_size_pix 32
#poetry run themeda_preproc -cores 2 to_chiplets -source_name rain -roi_name savanna -pad_size_pix 0
#poetry run themeda_preproc -cores 32 denan_chiplets -source_name rain -roi_name savanna -pad_size_pix 32
#poetry run themeda_preproc -cores 32 denan_chiplets -source_name rain -roi_name savanna -pad_size_pix 0
#poetry run themeda_preproc -cores 32 chiplets_to_geotiff -source_name rain -roi_name savanna
#xvfb-run poetry run themeda_preproc plot_maps -source_name rain -roi_name savanna
#poetry run themeda_preproc summary_stats -source_name rain -roi_name savanna
#poetry run themeda_preproc summary_stats -source_name rain -roi_name savanna --log_transformed

# land use
#poetry run themeda_preproc acquire -source_name land_use
#poetry run themeda_preproc prep -source_name land_use
#poetry run themeda_preproc to_chips -source_name land_use -roi_name savanna
#poetry run themeda_preproc to_chiplets -source_name land_use -roi_name savanna -pad_size_pix 32
#poetry run themeda_preproc to_chiplets -source_name land_use -roi_name savanna -pad_size_pix 0
#poetry run themeda_preproc -cores 32 chiplets_to_geotiff -source_name land_use -roi_name savanna
#xvfb-run poetry run themeda_preproc plot_maps -source_name land_use -roi_name savanna

# fire scar - early
#poetry run themeda_preproc prep -source_name fire_scar_early
#poetry run themeda_preproc to_chips -source_name fire_scar_early -roi_name savanna
#poetry run themeda_preproc to_chiplets -source_name fire_scar_early -roi_name savanna -pad_size_pix 32
#poetry run themeda_preproc to_chiplets -source_name fire_scar_early -roi_name savanna -pad_size_pix 0
#poetry run themeda_preproc -cores 32 chiplets_to_geotiff -source_name fire_scar_early -roi_name savanna
#xvfb-run poetry run themeda_preproc plot_maps -source_name fire_scar_early -roi_name savanna

# fire scar - late
#poetry run themeda_preproc prep -source_name fire_scar_late
#poetry run themeda_preproc to_chips -source_name fire_scar_late -roi_name savanna
#poetry run themeda_preproc to_chiplets -source_name fire_scar_late -roi_name savanna -pad_size_pix 32
#poetry run themeda_preproc to_chiplets -source_name fire_scar_late -roi_name savanna -pad_size_pix 0
#poetry run themeda_preproc -cores 32 chiplets_to_geotiff -source_name fire_scar_late -roi_name savanna
#xvfb-run poetry run themeda_preproc plot_maps -source_name fire_scar_late -roi_name savanna

# elevation
#poetry run themeda_preproc acquire -source_name elevation
#poetry run themeda_preproc prep -source_name elevation
#poetry run themeda_preproc to_chips -source_name elevation -roi_name savanna
#poetry run themeda_preproc -cores 2 to_chiplets -source_name elevation -roi_name savanna -pad_size_pix 32
#poetry run themeda_preproc -cores 2 to_chiplets -source_name elevation -roi_name savanna -pad_size_pix 0
#poetry run themeda_preproc -cores 32 denan_chiplets -source_name elevation -roi_name savanna -pad_size_pix 32
#poetry run themeda_preproc -cores 32 denan_chiplets -source_name elevation -roi_name savanna -pad_size_pix 0
#poetry run themeda_preproc -cores 32 chiplets_to_geotiff -source_name elevation -roi_name savanna
#xvfb-run poetry run themeda_preproc plot_maps -source_name elevation -roi_name savanna
#poetry run themeda_preproc summary_stats -source_name elevation -roi_name savanna

# soil depth
#poetry run themeda_preproc acquire -source_name soil_depth
#poetry run themeda_preproc prep -source_name soil_depth
#poetry run themeda_preproc to_chips -source_name soil_depth -roi_name savanna
#poetry run themeda_preproc -cores 2 to_chiplets -source_name soil_depth -roi_name savanna -pad_size_pix 32
#poetry run themeda_preproc -cores 2 to_chiplets -source_name soil_depth -roi_name savanna -pad_size_pix 0
#poetry run themeda_preproc -cores 32 denan_chiplets -source_name soil_depth -roi_name savanna -pad_size_pix 32
#poetry run themeda_preproc -cores 32 denan_chiplets -source_name soil_depth -roi_name savanna -pad_size_pix 0
#poetry run themeda_preproc -cores 32 chiplets_to_geotiff -source_name soil_depth -roi_name savanna
#xvfb-run poetry run themeda_preproc plot_maps -source_name soil_depth -roi_name savanna
#poetry run themeda_preproc summary_stats -source_name soil_depth -roi_name savanna

# soil clay
#poetry run themeda_preproc acquire -source_name soil_clay
#poetry run themeda_preproc prep -source_name soil_clay
#poetry run themeda_preproc to_chips -source_name soil_clay -roi_name savanna
#poetry run themeda_preproc -cores 2 to_chiplets -source_name soil_clay -roi_name savanna -pad_size_pix 32
#poetry run themeda_preproc -cores 2 to_chiplets -source_name soil_clay -roi_name savanna -pad_size_pix 0
#poetry run themeda_preproc -cores 32 denan_chiplets -source_name soil_clay -roi_name savanna -pad_size_pix 32
#poetry run themeda_preproc -cores 32 denan_chiplets -source_name soil_clay -roi_name savanna -pad_size_pix 0
#poetry run themeda_preproc -cores 32 chiplets_to_geotiff -source_name soil_clay -roi_name savanna
#xvfb-run poetry run themeda_preproc plot_maps -source_name soil_clay -roi_name savanna
#poetry run themeda_preproc summary_stats -source_name soil_clay -roi_name savanna

# soil ece
#poetry run themeda_preproc acquire -source_name soil_ece
#poetry run themeda_preproc prep -source_name soil_ece
#poetry run themeda_preproc to_chips -source_name soil_ece -roi_name savanna
#poetry run themeda_preproc -cores 2 to_chiplets -source_name soil_ece -roi_name savanna -pad_size_pix 32
#poetry run themeda_preproc -cores 2 to_chiplets -source_name soil_ece -roi_name savanna -pad_size_pix 0
#poetry run themeda_preproc -cores 32 denan_chiplets -source_name soil_ece -roi_name savanna -pad_size_pix 32
#poetry run themeda_preproc -cores 32 denan_chiplets -source_name soil_ece -roi_name savanna -pad_size_pix 0
#poetry run themeda_preproc -cores 32 chiplets_to_geotiff -source_name soil_ece -roi_name savanna
#xvfb-run poetry run themeda_preproc plot_maps -source_name soil_ece -roi_name savanna
#poetry run themeda_preproc summary_stats -source_name soil_ece -roi_name savanna

# hash db formation
# poetry run themeda_preproc form_hash_db -hash_db_path themeda_preproc_file_db_smb_20231020.json

poetry run themeda_preproc pad_chiplets -chiplets_path ~/preproc/data/chiplets/roi_savanna/pad_0/land_cover/chiplets_land_cover_2020_roi_savanna_pad_0.npy -output_path ~/preproc/mnt/analysis/pad_test.npy -dtype uint8 -roi_name savanna  


