# This script is called at the head node
# Call workers to create CPDs based on cluster config


from subprocess import getstatusoutput
import json
from sys import argv
import argparse

def call_worker(wid, conf):
    host       = conf['workers'][wid]
    name       = f"worker-{wid}"
    input      = conf["xy_file"]
    partmethod = conf["partmethod"]
    partkey    = conf["partkey"]
    maxworker  = len(conf["workers"])
    outdir     = conf["outdir"]
    projdir    = conf["projectdir"]
    cd         = f"cd {projdir}"
    makecpd    = f"./bin/make_cpd_auto --input {input} --partition {partmethod} --partkey {partkey} --workerid {wid} --maxworker {maxworker} --outdir {outdir}"
    cmd        = f"ssh {host} \"{cd}; tmux new -As {name} -d '{makecpd}'\""
    print(cmd)
    code, out = getstatusoutput(cmd)
    if code:
        print (code, out)

def test():
    test_conf = {
      "nfs": "/tmp",
      "partmethod": "div",
      "partkey": 10,
      "outdir": "./index",
      "xy_file": "./data/melb-both.xy",
      "scenfile": "./data/full.scen",
      "diffs": "./data/melb-both.xy.diff",
      "projectdir": "~/projects/Time-Dependant-Oracle-Search-RA/distributed-oracle-search"
    }

    maxworker = 9000
    test_conf["workers"] = ["localhost" for i in range(maxworker)]
    call_worker(0, test_conf)

def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", type=str, default="./example-cluster-conf.json", help="cluster config")
    parser.add_argument("-w", type=int, default=-1, help="specify the worker, -1 means run on all workers")
    parser.add_argument("-t", action='store_true',  help="call the testing function")
    args = parser.parse_args(argv[1:])

    if args.t:
        test()
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
    main(argv)
