#!/usr/bin/env bash

# ROI prep
poetry run ecofuture_preproc roi_prep -roi_name savanna
poetry run ecofuture_preproc roi_prep -roi_name australia

# DEA land cover
poetry run ecofuture_preproc acquire -source_name land_cover
poetry run ecofuture_preproc prep -source_name land_cover
poetry run ecofuture_preproc to_chips -source_name land_cover -roi_name savanna

# chiplet table prep
poetry run ecofuture_preproc chiplet_table_prep -roi_name savanna -pad_size_pix 32
poetry run ecofuture_preproc chiplet_table_prep -roi_name savanna -pad_size_pix 0

# land cover chiplets
poetry run ecofuture_preproc to_chips -source_name land_cover -roi_name savanna -pad_size_pix 32
poetry run ecofuture_preproc to_chips -source_name land_cover -roi_name savanna -pad_size_pix 0

# ANU climate - tmax
poetry run ecofuture_preproc acquire -source_name tmax
poetry run ecofuture_preproc prep -source_name tmax
poetry run ecofuture_preproc to_chips -source_name tmax -roi_name savanna
poetry run ecofuture_preproc to_chiplets -source_name tmax -roi_name savanna -pad_size_pix 32
poetry run ecofuture_preproc to_chiplets -source_name tmax -roi_name savanna -pad_size_pix 0

# ANU climate - rain
poetry run ecofuture_preproc acquire -source_name rain
poetry run ecofuture_preproc prep -source_name rain
poetry run ecofuture_preproc to_chips -source_name rain -roi_name savanna
poetry run ecofuture_preproc to_chiplets -source_name rain -roi_name savanna -pad_size_pix 32
poetry run ecofuture_preproc to_chiplets -source_name rain -roi_name savanna -pad_size_pix 0

# land use
poetry run ecofuture_preproc acquire -source_name land_use
poetry run ecofuture_preproc prep -source_name land_use
poetry run ecofuture_preproc to_chips -source_name land_use -roi_name savanna
poetry run ecofuture_preproc to_chiplets -source_name land_use -roi_name savanna -pad_size_pix 32
poetry run ecofuture_preproc to_chiplets -source_name land_use -roi_name savanna -pad_size_pix 0

# fire scar - early
poetry run ecofuture_preproc prep -source_name fire_scar_early
poetry run ecofuture_preproc to_chips -source_name fire_scar_early -roi_name savanna
poetry run ecofuture_preproc to_chiplets -source_name fire_scar_early -roi_name savanna -pad_size_pix 32
poetry run ecofuture_preproc to_chiplets -source_name fire_scar_early -roi_name savanna -pad_size_pix 0

# fire scar - late
poetry run ecofuture_preproc prep -source_name fire_scar_late
poetry run ecofuture_preproc to_chips -source_name fire_scar_late -roi_name savanna
poetry run ecofuture_preproc to_chiplets -source_name fire_scar_late -roi_name savanna -pad_size_pix 32
poetry run ecofuture_preproc to_chiplets -source_name fire_scar_late -roi_name savanna -pad_size_pix 0

# elevation
poetry run ecofuture_preproc acquire -source_name elevation
poetry run ecofuture_preproc prep -source_name elevation
poetry run ecofuture_preproc to_chips -source_name elevation -roi_name savanna
poetry run ecofuture_preproc to_chiplets -source_name elevation -roi_name savanna -pad_size_pix 32
poetry run ecofuture_preproc to_chiplets -source_name elevation -roi_name savanna -pad_size_pix 0

