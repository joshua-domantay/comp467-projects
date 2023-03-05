# Joshua Anthony Domantay
# Kevin Chaja
# COMP 467 - 21333
# 5 March 2023
# Import / export data

import os
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--job", dest="jobFolder", help="job to process")
parser.add_argument("--verbose", action="store_true", help="show verbose")
parser.add_argument("--TC", dest="timecode", help="Timecode to process")

args = parser.parse_args()

if args.jobFolder is None:
    print("No job selected")
    sys.exit(2)

if args.verbose:
    print("Hello")

f = open(os.path.abspath(args.jobFolder), "r")
print(os.path.realpath(args.jobFolder))
print(f.readline())
