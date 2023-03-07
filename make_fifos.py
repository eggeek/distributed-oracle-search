# This script is called at the head node
# Call workers to start fifo based on cluster config

from subprocess import getstatusoutput
import json
import argparse
from sys import argv

def call_worker(wid, conf):
    host       = conf['workers'][wid]
    name       = f"fifo-{wid}"
    xyfile     = conf["xy_file"]
    partmethod = conf["partmethod"]
    partkey    = conf["partkey"]
    maxworker  = len(conf["workers"])
    outdir     = conf["outdir"]
    projdir    = conf["projectdir"]
    diff       = conf['diffs'][0]
    cd         = f"cd {projdir}"
    alg        = "table-search"
    makefifo   = f"./bin/fifo_auto --input {xyfile} {diff} --partmethod {partmethod} --partkey {partkey} --workerid {wid} --maxworker {maxworker} --outdir {outdir} --alg {alg}"
    cmd        = f"ssh {host} \"{cd}; tmux new -As {name} -d '{makefifo}'\""
    print(cmd)
    code, out = getstatusoutput(cmd)
    if code:
        print (code, out)

def test(workerid):
    test_conf = {
      "nfs": "/tmp",
      "partmethod": "mod",
      "partkey": 100,
      "outdir": "./index",
      "xy_file": "./data/melb-both.xy",
      "scenfile": "./data/full.scen",
      "diffs": ["./data/melb-both.xy.diff"],
      "projectdir": "~/projects/Time-Dependant-Oracle-Search-RA/distributed-oracle-search"
    }

    maxworker = 100
    test_conf["workers"] = ["localhost" for i in range(maxworker)]
    call_worker(workerid, test_conf)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", type=str, default="./example-cluster-conf.json", help="cluster config")
    parser.add_argument("-w", type=int, default=-1, help="specify the worker, -1 means run on all workers")
    parser.add_argument("-t", action='store_true',  help="call the testing function")
    args = parser.parse_args(argv[1:])

    if args.t:
        test(0 if args.w == -1 else args.w)
        return

    conf_path = args.c
    worker = args.w
    cluster_conf = json.load(open(conf_path, "r"))

    if worker == -1:
        for i in range(len(cluster_conf['workers'])):
            call_worker(i, cluster_conf)
    else:
        call_worker(worker, cluster_conf)

if __name__ == "__main__":
    main()
