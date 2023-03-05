#
# This script is calling at the head node.
# Pass data directly to FIFOs
#
from timer import Timer
from args import args, process_filename, get_time_ns

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
node2worker = {}


def read_p2p(sce_name):
    """Read a point-to-point scenario file"""
    reqs = []

    with open(sce_name) as f:
        for line in f:
            if not line.strip() or line[0] != "q":
                continue
            reqs.append([int(x) for x in line.split()[1:]])

    return reqs


def make_parts(reqs, maxworker):
    """
        assign peuries to each worker, based on result from distribute controller
        return [
                [(s1, t1), (s2, t2), ...], // queries for worker 0
                [...], // queries for worker 1 
                ...
        ]
        where targets are in the assigned worker
    """

    cmd = f"./bin/gen_distribute_conf --nodenum {nodenum} --maxworker {maxworker} --partmethod {partmethod} --partkey {partkey}"
    code, out = getstatusoutput(cmd)
    if code:
        return code, out
    lines = out.split('\n')[1:]
    for l in lines:
        node, wid, bid, bidx = map(int, l.split(','))
        node2worker[node] = wid
    groups = defaultdict(list)
    worker = {}
    for s, t in reqs:
        wid = node2worker[t]
        assert(wid is not None)
        groups[wid].append([s, t])
    parts = [groups[i] for i in range(maxworker)]
    return code, parts


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

def get_node_num(xyfile):
    with open(xyfile, "r") as f:
        line = f.readlines()[3]
        _, num, _, _ = line.split(' ')
    return int(num)

def main(args):
    conf       = json.load("./part-example-conf.json")
    sce_name   = conf['scenfile']
    diffs      = conf['diffs']
    hosts      = conf['workers']
    partmethod = conf['partmethod']
    partkey    = conf['partkey']
    nfs        = conf['nfs']
    nodenum    = get_node_num(conf['xy_file'])
    maxworker  = len(hosts)

    with Timer() as r:
        reqs   = read_p2p(sce_name)

    total_qs = len(reqs)

    worker_conf = {
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
        code, parts = make_parts(reqs, maxworker)
        if code:
            print(code, out)
            exit(1)

    with Timer() as p:
        stats = []
        # Run one experiment per diff
        for i, dname in enumerate(diffs):
            workload = zip(hosts, cycle([nfs]), cycle([worker_conf]), cycle([dname]), parts)
            with Pool(maxworker) as pool:
                results = [
                    pool.apply_async(send_queries, worker)
                    for worker in workload if len(worker[-1]) > 0
                ]
                stats.append([res.get() for res in results])

    data = {
        "num_queries": total_qs,
        "num_partitions": maxworker,
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
