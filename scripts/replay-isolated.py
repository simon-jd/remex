#!/usr/bin/env python3
"""Fail closed inside unshare, then exec the original exported CPU action."""
import argparse
import json
import os
from pathlib import Path
import socket

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--receipt', required=True, type=Path)
parser.add_argument('--outer-netns', required=True)
parser.add_argument('capsule', type=Path)
args = parser.parse_args()
interfaces = socket.if_nameindex()
netns = os.readlink('/proc/self/ns/net')
if netns == args.outer_netns or len(interfaces) != 1 or interfaces[0][1] != 'lo':
    raise SystemExit('isolated replay namespace gate failed')
if os.environ.get('CUDA_VISIBLE_DEVICES') != '':
    raise SystemExit('CPU-only environment required')
with args.receipt.open('x') as stream:
    json.dump({'network_namespace': netns, 'outer_namespace': args.outer_netns,
               'interfaces': interfaces, 'entry_gate': 'passed',
               'scope': 'Actual namespace entry before exec; separate from action exit status'}, stream, indent=2)
    stream.flush()
    os.fsync(stream.fileno())
os.execve('/bin/bash', ['/bin/bash', str(args.capsule / 'run.sh')], dict(os.environ))
