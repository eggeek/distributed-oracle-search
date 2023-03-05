# Distributed Oracle Search

This repository is organised into three parts:

1. `.` The Python code for the driver.
2. `pathfinding/` The C++ code for `warthog`, a pathfinding library. We will use
   program `fifo`.
3. `data/` The graph representation of Melbourne's road network under the
   free-flow assumption and with congestion, and the full scenario file.

## Cluster Configuration

- Keys:
  - `workers`: host name of each worker, and you must have ssh access from the head node.
  When running experiments locally (e.g., for testing purpose), you can set them to `localhost`, and you must also have ssh server started on the localhost `ssh localhost`). You can also set it to `[localhost, localhost, ...]` to run distributed tasks locally.
  - `nfs`: the location of network file system that the head node and all workers can access to.
  - `partmethod, partkey`: this pair defines how to distribute nodes to workers, current supported methods are:
    - `div, <int>`
    - `mod, <int>`
    - see details in `./pathfinding/warthog/src/util/distribution_controller.h`
  - `outdir`: the directory of precomputed index
  - `xy_file`: the path of input graph
  - `scenfile`: the path of queries
  - `diffs`: the path of diff files, for experiments on perturbed graphs
  - `projectdir`: the directory of the project, will be used after ssh to the worker

- See an example [here](./part-example-conf.json)

## To compile the code

```sh
./install.sh dev
./install.sh fast
```
Compile executable with "dev" or "fast" flag, and copy executables to folder "./bin".

You can also compile executables manually:

``` sh
cd pathfinding/warthog && make fast [-j]
```

The `Makefile` should take care of compiling executables in `build/fast/bin`.

You will need Python 3.

## Auto Partitioning

Preprocessing and query processing tasks are distributed to multiple workers.
We use `./pathfinding/warthog/src/util/distribution_controller.h` to control the partitioning. 

We use a program (**TBD**) to generate a configuration file to describe which nodes are send to which worker and it will be sent to Python scripts.

## Computing a CPD

To compute a CPD, use the executable created above, and call it on worker.
In the below example, we compute cpd for the **subset** of nodes of input graph `melb-both.xy`,
where the **subset** is defined by the partitioning method (`--partition div --partkey 5`) and the maximum number of workers (`--maxworker 5`).

``` sh
bin/make_cpd --input melb-both.xy --partition div --partkey 5 --workerid 1 --maxworker 5
```

It will create one or more CPDs, and their filenames are auto-generated (see in `./pathfinding/warthog/programs/make_cpd_auto.cpp`).
The default directory of these CPDs are same as the input file, this can be controlled with the `--outdir` parameter (e.g., `--outdir ./index`).

- **Note:** The script will run with all available threads.

## To run the code

First, start a resident process (worker) with:

``` sh
bin/fifo --input <graph.xy> --alg [ cpd-search | cpd | ch ]
```

The executable `fifo` expects three files:

1. `<graph>.xy` Original xy-graph.
2. `<graph>.xy.diff` The modified (congested) xy-graph.
3. `<graph>.xy.cpd` The CPD for the original graph.

You can specify three entries after `--input` if your files do not respect this
convention.

- **Note:** This will run with all available threads by default, this can be
  disabled by compiling with `-DSINGLE_THREAD`. But, this does not do any work
  while waiting -- i.e., we simply build one search object per thread. You can
  then specify how many threads an experiment has access to with a flag on the
  driver; see below.

Then, we can run experiments with:

``` sh
python offline.py --scenario <file.scen>
```

You can then set further parameters to tweak the experiments, please use `python
offline.py --help` for a list of valid parameters.

- **Note:** The experiments script can only handle *one partition per worker*,
  or a single one on the driver. If you run more than that, you may end up in a
  deadlock (with multiple writers garbling the data on the FIFO).

### Runing without congestion

For algorithms that do not handle congestion (CH and CPD extractions), you need
only provide one entry if using the default setup, or two if you have a custom
setup.
