import argparse
import logging
from os.path import isfile, join

parser = argparse.ArgumentParser(description="Process some integers.")

parser.add_argument("-v", "--verbose", action="count", default=0)
parser.add_argument("-D", "--debug", action="store_true")

parts = parser.add_mutually_exclusive_group()
parts.add_argument(
    "-p",
    "--num-partitions",
    type=int,
    help="Number of partitions for processing the trip file.",
)
parts.add_argument(
    "-s",
    "--size-partitions",
    type=int,
    help="Number of elements per partition for processing the trip file.",
)

# K-moves are only available with extractions while hScale only influences A*
path = parser.add_argument_group("A* search")
path.add_argument(
    "-k",
    "--k-moves",
    type=int,
    default=-1,
    help="Number of moves to extract, default is -1 (all)",
)
path.add_argument(
    "--h-scale", default=1.0, type=float, help="Heuristic tolerance factor for A*."
)
path.add_argument(
    "--f-scale", default=0.0, type=float, help="Sub-optimality factor for A*."
)
path.add_argument(
    "--group",
    type=str,
    choices=["all", "mod", "div"],
    help="How to generate partitions, nothing means by range",
)
path.add_argument(
    "--sort", action="store_true", help="Sort partitions on targets before sending"
)

path.add_argument("--s-lim", default=0, type=int, help="Time limit in seconds")
path.add_argument("--ms-lim", default=0, type=int, help="Time limit in milliseconds")
path.add_argument("--us-lim", default=0, type=int, help="Time limit in mcroseconds")
path.add_argument("--ns-lim", default=0, type=int, help="Time limit in nanoseconds")

batch = parser.add_argument_group("batching")
batch.add_argument("-o", "--output", help="File to write output to.")
batch.add_argument(
    "-i",
    "--interface",
    choices=["rdd", "dataframe"],
    default="rdd",
    help="Which Spark interface to use.",
)
batch.add_argument(
    "-M", "--multi", action="store_true", help="Run the CPD searches in C++"
)
batch.add_argument(
    "--omp", type=int, default=0, help="Number of OpenMP threads for the C++ code"
)

stream = parser.add_argument_group("streaming")
stream.add_argument(
    "-B",
    "--no-broadcast",
    action="store_true",
    help="Use RDD instead of Spark broadcast variable for CPD.",
)
stream.add_argument(
    "-l",
    "--load",
    type=str,
    choices=["file", "byte", "list"],
    default="byte",
    help="\n".join(
        [
            "Method to load the CPD:",
            " - pass the file to search;",
            " - load the file as byte string in an RDD;",
            " - load the file in Python and save lists in an RDD.",
        ]
    ),
)
stream.add_argument(
    "--tick", type=int, default=3, help="Time between streaming ticks, in seconds."
)

files = parser.add_argument_group("files")
files.add_argument(
    "-b", "--base", type=str, default=".", help="Base directory the code is run from."
)
files.add_argument(
    "-d",
    "--dir",
    type=str,
    default="astar-early-stop/DynamicPathFinding/src/test/resources/",
    help="Directory containing the map files.",
)
files.add_argument("-m", "--map", type=str, default="square01.map", help="Map to use.")
files.add_argument(
    "-c", "--cpd", type=str, default="test-square01.map", help="CPD file to load."
)
files.add_argument(
    "--scenario",
    type=str,
    default="square01.map.scen",
    help="Scenario file to read from",
)
files.add_argument("--order", type=str, help="File to overwrite the NodeOrdering")
files.add_argument("--diff", type=str, help="File with travel time diff to use search")

rand = parser.add_argument_group("random")
rand.add_argument("-R", "--random", action="store_true", help="Randomise the seed.")
rand.add_argument(
    "--seed", type=int, default=562410645, help="Seed for the random generator"
)

server = parser.add_argument_group("server")
server.add_argument(
    "--host", type=str, default="localhost", help="Server to connect to."
)
server.add_argument(
    "--port", type=int, default=9999, help="Port to send information to on the server."
)

fifo = parser.add_argument_group("fifo")
fifo.add_argument(
    "--fifo",
    type=str,
    default="/tmp/warthog.fifo",
    help="Named pipe to communicate with warthog process",
)
fifo.add_argument(
    "--local",
    type=str,
    nargs="+",
    help="Named pipes opened on a network drive, "
    "if 'localhost' will alter the script to run locally.",
)
fifo.add_argument(
    "--cutoff",
    type=int,
    default=10000,
    help="How many queries do we need before distributing work.",
)
fifo.add_argument(
    "--thread-alloc",
    action="store_true",
    help="Use thread allocation on the FIFO receiver.",
)
fifo.add_argument(
    "--nfs", type=str, default="/srv/data", help="Network drive to write queries to."
)
fifo.add_argument(
    "--diffs",
    type=str,
    nargs="+",
    default="-",
    help="Diff files for congestion updates, '-' means no update.",
)
fifo.add_argument(
    "--no-cache", action="store_true", help="Disable runtime cache in workers."
)

modus = parser.add_mutually_exclusive_group()
modus.add_argument("--div", type=int, help="Assign nodes to $#host = target / div$")
modus.add_argument("--mod", type=int, help="Assign nodes to $#host = target %% mod$")
modus.add_argument(
    "--alloc",
    type=int,
    nargs="+",
    help="Range of nodes read as (0, n, m, ...) and assign to " "host1, host2, ...",
)

logging.basicConfig()
Log = logging.getLogger(__name__)

args = parser.parse_known_args()[0]

if args.verbose == 0:
    Log.setLevel(logging.WARN)
elif args.verbose == 1:
    Log.setLevel(logging.INFO)
elif args.verbose == 2:
    Log.setLevel(logging.DEBUG)


def process_filename(fname):
    if isfile(fname):
        return fname
    else:
        with_dir = join(args.base, args.dir, fname)

        if isfile(with_dir):
            return with_dir
        else:
            raise IOError("File {} not found, searched {}.".format(fname, with_dir))


def get_time_ns(args):
    """Get the time argument as nanoseconds"""
    tlim = args.ns_lim

    if args.s_lim > 0:
        tlim = args.s_lim * 1e9
    elif args.ms_lim > 0:
        tlim = args.ms_lim * 1e6
    elif args.us_lim > 0:
        tlim = args.us_lim * 1e3

    return tlim
