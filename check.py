#!/usr/bin/env python3

__author__ = 'mtravis'

import ripplepy
import argparse
import sys
import json
import socket
import csv
import time

# main
argparser = argparse.ArgumentParser()
argparser.add_argument("-c", "--connection", type=str, required=True)
argparser.add_argument("-s", "--start", type=int, required=True)
argparser.add_argument("-e", "--end", type=int)
argparser.add_argument("-o", "--output", type=str, required=True)
argparser.add_argument("-t", "--timeout", type=int, default=60)
args = argparser.parse_args()

hostname = socket.gethostname()

if args.end is None:
    args.end = args.start

outfile = csv.writer(open(args.output, 'a', newline=''), delimiter='\t')

ripd = ripplepy.Ripple(args.connection, timeout=args.timeout)

for i in range(args.start, args.end+1):
    try:
        ledger = ripd.cmd_ledger(i)
        outfile.writerow([hostname, time.time(), i, ledger["result"]["ledger"]["seqNum"], ledger["result"]["ledger"]["ledger_hash"],
                          ledger["result"]["ledger"]["parent_hash"]])
    except:
        outfile.writerow([hostname, time.time(), i, 0])
