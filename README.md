# Distributed Oracle Search

This repository is organised into three parts:

1. `.` The Python code for the driver.
2. `pathfinding/` The C++ code for `warthog`, a pathfinding library. We will use
   program `fifo`.
3. `data/` The graph representation of Melbourne's road network under the
   free-flow assumption and with congestion, and the full scenario file.

## TLDR

You can quickly run a demo by following steps:

1. `./install.sh fast`: compile executable in `pathfinding` and copy them to `./bin` 
2. `python make_cpds.py -t`: precompute CPDs on different workers based on a predefined testing configuration file (`-t`)
3. `python make_fifos.py -t`: start a resident service on each worker based on a predefined testing configuration file (`-t`)
3. `python process_query.py -t`: assign queries to each worker based on a predefined testing configuration file (`-t`)

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

- Example:
  ```json
  {
    "workers": ["localhost", "localhost", "localhost"],
    "nfs": "/tmp",
    "projectdir": "~/projects/Time-Dependant-Oracle-Search-RA/distributed-oracle-search/",
    "partmethod": "mod",
    "partkey": 3,
    "outdir": "./index",
    "xy_file": "./data/melb-both.xy",
    "scenfile": "./data/full.scen",
    "diffs": ["./data/melb-both.xy.diff"]
  }
  ```

## To compile the code

```sh
./install.sh dev
./install.sh fast
```
Compile executable with "dev" (for testing) or "fast" flag, and copy executables to folder "./bin".

You can also compile executables manually:

``` sh
cd pathfinding/warthog && make fast [-j]
cd pathfinding/warthog && make dev [-j]
```

The `Makefile` should take care of compiling executables in `build/fast/bin`.

You will need Python 3.

## Auto Partitioning

Preprocessing and query processing tasks are distributed to multiple workers.
We use `./pathfinding/warthog/src/util/distribution_controller.h` to control the partitioning. 

We use a program (`./bin/gen_distribute_conf`) to generate a configuration file to describe which nodes are send to which worker and it will be sent to Python scripts.

## Computing a CPD

To compute a CPD, use the executable created above, and call it on worker.
In the below example, we compute cpd for the **subset** of nodes of input graph `melb-both.xy`,
where the **subset** is defined by the partitioning method (`--partition div --partkey 5`) and the maximum number of workers (`--maxworker 5`).

``` sh
bin/make_cpd_auto --input melb-both.xy --partition div --partkey 5 --workerid 1 --maxworker 5
```

It will create one or more CPDs, and their filenames are auto-generated (see in `./pathfinding/warthog/programs/make_cpd_auto.cpp`).
The default directory of these CPDs are same as the input file, this can be controlled with the `--outdir` parameter (e.g., `--outdir ./index`).

- **Note:** The script will run with all available threads.

To create CPDs on different workers, you can run this script at the head node:

```sh
python make_cpds.py -c ./example-cluster-conf.json
```

or manually ssh to each worker and run the `./bin/make_cpd_auto` command.

## To run the code

You can manually ssh to each worker and start a resident process with:

``` sh
./bin/fifo_auto --input ./data/melb-both.xy ./data/melb-both.xy.diff --partition mod --partkey 100 --workerid 0 --maxworker 100 --outdir ./index --alg table-search
```

or run the script at the head node:

```sh
python make_fifos.py -c ./example-cluster-conf.json
```

Then, we can run experiments with:

``` sh
python process_query.py -c ./example-cluster-conf.json
```

- **Note:** The experiments script can only handle *one partition per worker*,
  or a single one on the driver. If you run more than that, you may end up in a
  deadlock (with multiple writers garbling the data on the FIFO).

## TODO

- Running without congestion

  For algorithms that do not handle congestion (CH and CPD extractions), you need
  only provide one entry if using the default setup, or two if you have a custom
  setup.

- Multiple partitions on same worker

  Current implementation only support one partition per worker.

