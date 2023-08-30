#!/usr/bin/env bash

# ROI prep
poetry run ecofuture_preproc roi_prep -roi_name savanna
poetry run ecofuture_preproc roi_prep -roi_name australia

# DEA land cover
poetry run ecofuture_preproc acquire -source_name land_cover
poetry run ecofuture_preproc prep -source_name land_cover
poetry run ecofuture_preproc to_chips -source_name land_cover -roi_name savanna

# ANU climate
poetry run ecofuture_preproc acquire -source_name rain
poetry run ecofuture_preproc acquire -source_name tmax

# land use
poetry run ecofuture_preproc acquire -source_name land_use

# fire scar
poetry run ecofuture_preproc acquire -source_name fire_scar
