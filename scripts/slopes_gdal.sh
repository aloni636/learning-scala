gdalwarp -t_srs EPSG:32645 -r bilinear -of VRT data/Everest_COP30.tif /vsistdout/ \
  | gdaldem slope /vsistdin/ /vsistdout/ \
  | gdaldem color-relief /vsistdin/ scripts/viridis.txt visualizations/slopes_gdal.png -of PNG
