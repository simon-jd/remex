#!/usr/bin/env python3
"""Count original Bazel 9.3 LogEntry records (varint-delimited protobuf).
Schema: tests/protocol/fixtures/bazel_9_3_remote_execution_log.proto.
Only declared method/status/time fields are decoded; unknown fields are skipped.
"""
import argparse
import collections
import json
from pathlib import Path

p=argparse.ArgumentParser()
p.add_argument('logs',nargs='+',type=Path)
a=p.parse_args()
def varint(data,at):
    n=0
    for shift in range(0,70,7):
        if at>=len(data):raise ValueError('truncated varint')
        b=data[at];at+=1;n|=(b&127)<<shift
        if not b&128:return n,at
    raise ValueError('oversized varint')
def fields(data):
    at=0
    while at<len(data):
        key,at=varint(data,at);number,kind=key>>3,key&7
        if number==0:raise ValueError('zero field number')
        if kind==0:value,at=varint(data,at)
        elif kind in (1,5):
            size=8 if kind==1 else 4;value=data[at:at+size];at+=size
            if len(value)!=size:raise ValueError('truncated fixed field')
        elif kind==2:
            size,at=varint(data,at);value=data[at:at+size];at+=size
            if len(value)!=size:raise ValueError('truncated bytes')
        else:raise ValueError('unsupported protobuf wire type')
        yield number,kind,value
def scalar(data,number,default=0):
    result=default
    for n,k,v in fields(data):
        if n==number and k==0:result=v
    return result
def timestamp(data):
    return scalar(data,1)+scalar(data,2)/1e9
reports=[]
for file in a.logs:
    data=file.read_bytes();at=0;methods=collections.Counter();statuses=collections.Counter();durations=collections.defaultdict(list)
    while at<len(data):
        size,at=varint(data,at)
        if size>64<<20 or at+size>len(data):raise ValueError('truncated/oversized log entry')
        entry=data[at:at+size];at+=size
        values={n:v for n,k,v in fields(entry) if k==2}
        method=values.get(3,b'').decode('utf8')
        if '/' not in method:raise ValueError('LogEntry has no full method name')
        status=scalar(values.get(2,b''),1)
        methods[method]+=1;statuses[f'{method} status={status}']+=1
        if 5 in values and 6 in values:
            duration=timestamp(values[6])-timestamp(values[5])
            if duration>=0:durations[method].append(duration)
    reports.append({'file':str(file),'records':sum(methods.values()),'methods':dict(sorted(methods.items())),
                    'statuses':dict(sorted(statuses.items())),
                    'rpc_duration_seconds':{m:{'count':len(v),'mean':sum(v)/len(v),'p95':sorted(v)[min(len(v)-1,int(len(v)*.95))]} for m,v in sorted(durations.items())}})
print(json.dumps(reports,indent=2))
