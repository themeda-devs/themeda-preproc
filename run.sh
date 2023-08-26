#!/usr/bin/env bash

# ROI prep
poetry run ecofuture_preproc roi_prep -roi_name savanna
poetry run ecofuture_preproc roi_prep -roi_name australia

# DEA land cover
poetry run ecofuture_preproc acquire -source_name land_cover
