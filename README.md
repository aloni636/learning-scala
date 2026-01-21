# Workflow
`sbt` is your friend - that's how you execute scala:
```sh
sbt
sbt:learning-scala> run
```

## Running Exercises
I use a **cli argument dispatcher**, so you select an exercise by it's object name and run it: `sbt "run Ex01"`. View all available exercises by running `sbt run`.

## Creating Exercises
Create a new `Ex<>` file in `hello/src/main/scala`, and add an `Exercise` object with a `run` method. After that you should add it to the exercise list in `hello/src/main/scala/hello` and run it with `sbt "run Ex<>"`.

## Running Spark Exercises
A spark standalone cluster is automatically started when the devcontainer is created, available at `spark://localhost:7077`. 

You can monitor the Spark cluster at `http://localhost:8080`, the history server at `http://localhost:18080`, and more granularly at `$SPARK_HOME/logs/`.

Experiment interactively with:
```sh
spark-shell --master spark://localhost:7077
```

A Jupyter-lab server is also automatically started (with Almond Scala kernel, Metals LSP support and jupytext).

Open it by looking for the Jupyter URL in `.jupyter.log` file, or run:

```sh
jupyter server list
# Currently running servers:
# http://127.0.0.1:8888/?token=<TOKEN> :: /workspaces/learning-scala
```

All notebooks are stored in `./notebooks` as jupytext files (those are easier to manage with git).

To stop Jupyter, run `./scripts/stop-jupyter.sh`.

Note: VSCode notebooks do not communicate correctly with Almond kernel, leading to non existent autocomplete support, so I recommend using Jupyter Lab directly.

# Downloading Data
*TLDR:*

```sh
source ./.venv/bin/activate
./scripts/download-taxi.sh
./scripts/download-himalayas.sh
```

Exercise 6 (`./src/main/scala/Ex06.scala`) requires part of the **NYC TLC dataset**. Fetch it using `./scripts/download-taxi.sh`.

Exercise 7 (`./src/main/scala/Ex07.scala`) requires the **Copernicus dataset**. Fetching it is complex enough to require Python scripting. The Python environment is automatically configured, but **the `.venv` must be activated!** If VSCode doesn't activate it when you open the integrated terminal, run `source ./.venv/bin/activate` before running `./scripts/download-himalayas.sh`

# Projections
In **exercise 7** (`Ex07`) I used a custom projection to perform projected analysis of the Himalayas without UTM stitching. To evaluate it (and other projections) I performed CRS distortion analysis which you can read about here: [**Projection Considerations**](PROJECTIONS.md).

# Debugging

## Dependencies Hell
Use **Graphviz Interactive Preview** extension to analyze & debug sbt's dependency graph:
```sh
sbt dependencyDot
# ...
# [info] Wrote dependency graph to '/workspaces/learning-scala/target/dependencies-compile.dot'
```

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
- https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf for NYC-TLC data dictionary PDF.
- https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/10m/physical/ne_10m_geography_regions_polys.zip for `./data/himalayas.geojson` (extracted with `ogr2ogr himalayas.geojson ne_10m_geography_regions_polys.shp -where "name = 'Himalayas'"`)