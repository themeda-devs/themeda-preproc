# Some thoughts:
#  * Use `fiona` to read the shp file
#  * Iterate over `handle` and read `.geometry.coordinates`
#  * Transform points into Albers
#  * Use `rasterio` to rasterise the geometries; how to define the base raster?
#  * Match chip resolution to that of the data (1km resolution)
#  * https://catalogue.data.wa.gov.au/dataset/noaa-fire-history-mapping
#  * Maybe loop through all the coordinates to work out the bounding-box and the
#    resolution?
#  * Might also be easier to use odc-geo:
#    https://odc-geo.readthedocs.io/en/latest/_api/odc.geo.xr.rasterize.html#odc.geo.xr.rasterize
#  * odc.geo.geom.polygon(next(h).geometry.coordinates[0],4326)
#  * odc.geo.xr.rasterize(g, 0.0001)
#  * actually, odc.geo.geom.multigeom([polygons]) and then rasterize
#  * but we want a count at each pixel, not just presence/absence
#  * rioxarray.merge.merge_arrays((...), method=rasterio.merge.copy_sum)
#  * need to set resolution also
#  * need to rename the dimensions to 'x' and 'y' rather than lat/lon
#  * next(h).properties["DATE"] to get the date of a burn (for wet/dry determination)
