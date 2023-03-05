#
# Pass data directly to FIFOs instead of using Spark.
#
from timer import Timer
from args import args, process_filename, get_time_ns
#  import tools.reader as reader

import os
import stat
from os.path import join, isdir
import json
import csv
from multiprocessing.dummy import Pool
from itertools import cycle
from subprocess import getstatusoutput
from collections import defaultdict


fifo = "/tmp/warthog.fifo"
answer = "/tmp/warthog.answer"


def read_p2p(sce_name):
    """Read a point-to-point scenario file"""
    reqs = []

    with open(sce_name) as f:
        for line in f:
            if not line.strip() or line[0] != "q":
                continue
            reqs.append([int(x) for x in line.split()[1:]])

    return reqs


def make_parts(reqs, which, num_parts, size_parts):
    if which == "all":
        # Group queries per destination, then partition
        groups = defaultdict(list)
        for (x, y) in reqs:
            groups[y].append([x, y])

        parts = [[] for _ in range(num_parts)]
        i = 0
        for v in groups.values():
            parts[i].extend(v)

            if len(parts[i]) > size_parts:
                i += 1
    elif which == "mod" or which == "div" or which == "alloc":
        parts = [[] for _ in range(num_parts)]

        for (x, y) in reqs:
            if which == "mod":
                key = y % size_parts
            elif which == "div":
                key = y // size_parts
            elif which == "alloc":
                key = next(i for i, val in enumerate(size_parts) if val > y)
            else:
                raise ValueError(f"Unknown alloc scheme '{which}'")

            parts[key].append([x, y])
    else:
        parts = [reqs[size_parts * i : size_parts * (i + 1)] for i in range(num_parts)]

    return parts


def send_local(qname, config):
    with open(fifo, "w") as f:
        f.write(config)

    out = ""
    os.mkfifo(answer)
    with open(answer, "r") as f:
        for line in f:
            out = line.strip()  # Should be a single line

    os.remove(answer)

    return 0, out


def send_remote(hostname, fname, qname, config):
    with open(fname, "w") as f:
        f.write(f"mkfifo {answer}\n")
        f.write(f"cat <<CONF > {fifo}\n")  # HEREDOC
        f.write(config)
        f.write("CONF\n")  # HEREDOC
        f.write(f"cat {answer}\n")
        f.write(f"rm {answer}")

    return getstatusoutput(f"ssh {hostname} 'bash -s' < {fname}")


def send_queries(hostname, nfs, config, dname, reqs):
    fname = f"query.{hostname}"
    qname = join(nfs, fname)  # Query files need to be unique
    nb_reqs = len(reqs)
    # Runtime configuration for the resident process(es)
    conf = json.dumps(config) + "\n" + "{} {} {}\n".format(qname, answer, dname)

    with Timer() as t_prepare:
        with open(qname, "w") as f:
            f.write(f"{nb_reqs}\n")
            f.writelines("{} {}\n".format(*x) for x in reqs)

    print(f"Processing {nb_reqs} queries on '{hostname}'")
    with Timer() as t_partition:
        if hostname == "localhost":
            code, out = send_local(qname, conf)
        else:
            code, out = send_remote(hostname, fname, qname, conf)

    if code == 0:
        res = out.split(",")
        os.remove(qname)
        if hostname != "localhost":
            os.remove(fname)
    else:
        print(code, out)
        res = ""

    return (*res, t_prepare.interval * 1e9, t_partition.interval * 1e9, len(reqs))


def read_p2p(sce_name):
    """Read a point-to-point scenario file"""
    reqs = []

    with open(sce_name) as f:
        for line in f:
            if not line.strip() or line[0] != 'q':
                continue
            reqs.append([int(x) for x in line.split()[1:]])

    return reqs

def main(args):
    sce_name = process_filename(args.scenario)

    # Debug settings
    if args.debug:
        args.omp = 1
        args.verbose = 2
        args.num_partitions = 1

    with Timer() as r:
        reqs = read_p2p(sce_name)

    total_qs = len(reqs)

    if args.num_partitions is not None:
        num_parts = args.num_partitions
    elif args.size_partitions is not None:
        num_parts = max(1, total_qs // args.size_partitions)
    else:
        num_parts = 5  # Default number of workers

    if len(reqs) < args.cutoff or args.local is None:
        # Force local execution
        hosts = ["localhost"]
        num_parts = 1
        nfs = "/tmp"
        div = None
        mod = None
        alloc = None
    else:
        hosts = args.local
        nfs = args.nfs
        div = args.div
        mod = args.mod
        alloc = args.alloc

    assert num_parts <= len(
        hosts
    ), "Currently only works with max 1 partition per worker."

    # Config passed to warthog threads
    conf = {
        "hscale": args.h_scale,
        "fscale": args.f_scale,
        "time": get_time_ns(args),
        "itrs": -1,
        "k_moves": args.k_moves,
        "threads": args.omp,
        "verbose": args.verbose > 0,
        "debug": args.debug,
        "thread_alloc": args.thread_alloc,
        "no_cache": args.no_cache,
    }

    print(f"Preparing to send {total_qs} queries to {hosts}.")

    with Timer() as w:
        if div is not None:
            num_parts = len(hosts)
            parts = make_parts(reqs, "div", num_parts, div)
            assert (
                len(parts) == num_parts
            ), "Can only use --div to produce as many parts as hosts"
        elif mod is not None:
            assert mod == len(hosts), "Can only use --mod with the same number of hosts"

            num_parts = args.mod
            parts = make_parts(reqs, "mod", num_parts, mod)
            assert not any(len(x) == 0 for x in parts)
        elif alloc is not None:
            assert len(alloc) == len(
                hosts
            ), "Can only use --alloc with the same number of hosts"
            num_parts = len(alloc)
            parts = make_parts(reqs, "alloc", num_parts, alloc)
        else:
            size_parts = (total_qs // num_parts) + 1  # We want inclusive partitions
            parts = make_parts(reqs, args.group, num_parts, size_parts)

        if args.sort:
            for l in parts:
                l.sort(key=lambda x: x[1])

    with Timer() as p:
        stats = []
        # Run one experiment per diff
        for i, dname in enumerate(args.diffs):
            workload = zip(hosts, cycle([nfs]), cycle([conf]), cycle([dname]), parts)

            with Pool(num_parts) as pool:  # number of workers is important
                results = [
                    pool.apply_async(send_queries, w)
                    for w in workload
                    if len(w[-1]) > 0
                ]
                stats.append([r.get() for r in results])

    data = {
        "num_queries": total_qs,
        "num_partitions": num_parts,
        "t_read": r.interval,
        "t_workload": w.interval,
        "t_process": p.interval,
    }

    # Header for partitions' results (in CSV)
    header = [
        "expe",
        "n_expanded",
        "n_inserted",
        "n_touched",
        "n_updated",
        "n_surplus",
        "plen",
        "finished",
        "t_receive",
        "t_astar",
        "t_search",
        "t_prepare",
        "t_partition",
        "size",
    ]

    if args.output is None:
        print(data)
        print(header)
        for i, expe in enumerate(stats):
            for row in expe:
                print(i, row)
    else:
        # Assume args.output is a directory
        dirname = args.output

        if not isdir(dirname):
            os.makedirs(dirname)

        # Save session metrics data in json format, try to get the same output
        # as the FlighRecorder.
        with open(join(dirname, "metrics.json"), "w") as f:
            json.dump(data, f)

        with open(join(dirname, "data.json"), "w") as f:
            json.dump(args.__dict__, f)

        with open(join(dirname, "parts.csv"), "w") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(header)
            writer.writerows([[i] + row for i, row in stats])


if __name__ == "__main__":
    main(args)
