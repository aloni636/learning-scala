# Workflow
`sbt run` is your friend - that's how you execute scala.
Make sure your'e in hello, `cd ./hello`.

# Running Exercises
I use a **cli argument dispatcher**, so you select an exercise by it's object name and run it: `sbt "run Ex01"`. View all available exercises by running `sbt run`.

# Credits
- https://github.com/datablist/sample-csv-files for `./data/customers-100.csv`.
- https://file-examples.com/index.php/sample-images-download/sample-tiff-download/ for `./data/0x00000001-0x00000002-0x00000001.tiff` (downsampled).
- https://people.math.sc.edu/Burkardt/data/tif/tif.html (at3_1m4_01.tif, biological cells, frame 1;) for `./data/0x00000001-0x00000002-0x00000003.tiff` (downsampled).
- https://portal.opentopography.org/raster?opentopoID=OTSDEM.032021.4326.3 for `./data/Everest_COP30.tif`. **Extent details (lon, lat)**:
    ```
    TL: [86.899444, 28.031667]
    TR: [86.983056, 28.031667]
    BR: [86.983056, 27.957778]
    BL: [86.899444, 27.957778]
    ```
