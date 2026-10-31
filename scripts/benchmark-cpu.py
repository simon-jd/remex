#!/usr/bin/env python3
"""Same-host, same-affinity cold/warm CPU benchmark with isolated project data."""
import argparse, hashlib, json, os, pathlib, shutil, signal, subprocess, tempfile, time, urllib.request
p=argparse.ArgumentParser();p.add_argument('--cores',default='8,9,10');p.add_argument('--jobs',type=int,default=3);p.add_argument('--output',type=pathlib.Path,required=True);a=p.parse_args()
root=pathlib.Path(__file__).resolve().parents[1];out=a.output.resolve();out.mkdir(parents=True,exist_ok=False)
project='remexbench'+str(os.getpid());cg=pathlib.Path(f'/sys/fs/cgroup/user.slice/user-{os.getuid()}.slice/user@{os.getuid()}.service')/project
cg.mkdir();(cg/'cgroup.subtree_control').write_text('+cpu +memory +pids\n')
env=dict(os.environ,REMEX_POSTGRES_PORT='35432',REMEX_CAS_PORT='39092',REMEX_CACHE_HTTP_PORT='38080',REMEX_CACHE_GB='4')
override=out/'compose.json';override.write_text(json.dumps({'services':{'postgres':{'cpuset':a.cores},'bazel-remote':{'cpuset':a.cores}}}))
compose=['docker','compose','-p',project,'-f',str(root/'deploy/compose.yaml'),'-f',str(override)]
procs=[];logs=[];bazel=None
def run(argv,log,cwd=root,check=True):
 with open(log,'w') as f:
  result=subprocess.run(argv,cwd=cwd,env=env,stdout=f,stderr=subprocess.STDOUT)
 if check and result.returncode:raise RuntimeError(f'{argv[0]} failed; see {log}')
 return result.returncode
def start(argv,name):
 log=open(out/(name+'.log'),'w');logs.append(log)
 proc=subprocess.Popen(['taskset','-c',a.cores,*argv],cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT);procs.append(proc);return proc
summary={'cpu_affinity':a.cores,'jobs':a.jobs,'control_plane_and_workers_share_affinity':True,'order':['local-cold','local-warm','remote-cold','remote-warm'],'runs':[]}
try:
 run(compose+['up','-d','--wait','postgres','bazel-remote'],out/'compose-up.log')
 start([str(root/'build/bin/remex-scheduler'),'--listen','127.0.0.1:39980','--admin','127.0.0.1:39081','--cas','127.0.0.1:39092','--postgres','postgres://remex:remex-local-only@127.0.0.1:35432/remex?sslmode=disable'],'scheduler')
 for n in range(100):
  try:urllib.request.urlopen('http://127.0.0.1:39081/healthz',timeout=1);break
  except OSError:time.sleep(.1)
 else:raise RuntimeError('scheduler health timeout')
 for i in range(a.jobs):start([str(root/'bazel-bin/worker/remex-worker'),'--scheduler','127.0.0.1:39980','--cas','127.0.0.1:39092','--cgroup-root',str(cg),'--root',str(out/f'worker-{i}'),'--id',f'{project}-{i}','--property','pool=cpu'],f'worker-{i}')
 time.sleep(1)
 bazel=['taskset','-c',a.cores,str(root/'.tools/bazel.official'),'--output_user_root='+str(out/'bazel-user'),'--output_base='+str(out/'bazel-output')]
 workspace=root/'samples/cpp-history'
 # Dependency fetching and analysis are explicitly outside timed build runs.
 run(bazel+['build','--nobuild','//:math_test','//:abseil_test','--lockfile_mode=off'],out/'dependency-setup.log',workspace)
 for name in summary['order']:
  run(bazel+['clean'],out/(name+'-clean.log'),workspace)
  folder=out/name;folder.mkdir()
  flags=[f'--jobs={a.jobs}','--lockfile_mode=off','--test_output=errors','--remote_download_outputs=all','--action_env=REMEX_BASELINE=equal-affinity',f'--build_event_json_file={folder}/bep.json',f'--execution_log_compact_file={folder}/execution.zstd']
  if name.startswith('local'):
   flags+=['--spawn_strategy=local','--strategy=TestRunner=local','--disk_cache='+str(out/'local-disk-cache')]
  else:
   flags+=['--spawn_strategy=remote','--strategy=TestRunner=remote','--noremote_local_fallback','--remote_executor=grpc://127.0.0.1:39980','--remote_cache=grpc://127.0.0.1:39092','--remote_instance_name=benchmark',f'--remote_grpc_log={folder}/grpc.bin']
  command=bazel+['test','//:math_test','//:abseil_test']+flags
  (folder/'command.json').write_text(json.dumps(command,indent=2)+'\n')
  start_time=time.monotonic();exit_code=run(command,folder/'bazel.log',workspace,False);elapsed=time.monotonic()-start_time
  digests={}
  for target in ('math_test','abseil_test'):
   binary=out/'bazel-output/execroot/_main/bazel-out/k8-fastbuild/bin'/target
   if binary.is_file():digests[target]=hashlib.sha256(binary.read_bytes()).hexdigest()
  item={'name':name,'exit_code':exit_code,'elapsed_seconds':elapsed,'output_sha256':digests};summary['runs'].append(item)
  (folder/'ledger.json').write_bytes(urllib.request.urlopen('http://127.0.0.1:39081/export').read())
  (out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(item),flush=True)
  if exit_code:raise RuntimeError(f'{name} failed')
 summary['missing_output_digests']=[x['name'] for x in summary['runs'] if len(x['output_sha256']) != 2]
 summary['all_output_digests_equal']=None if summary['missing_output_digests'] else len({json.dumps(x['output_sha256'],sort_keys=True) for x in summary['runs']})==1
 summary['limits']='One ordered four-run trial; warm dependency downloads, empty local disk cache and remote CAS/AC initially. Background OS work is not excluded. Remote DB/CAS/scheduler/workers/client and local client all share the recorded core affinity.'
 (out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
finally:
 if bazel is not None:run(bazel+['shutdown'],out/'bazel-shutdown.log',workspace,check=False)
 for proc in procs:proc.terminate()
 for proc in procs:
  try:proc.wait(timeout=10)
  except subprocess.TimeoutExpired:proc.kill();proc.wait()
 for log in logs:log.close()
 run(compose+['logs','--no-color'],out/'compose.log',check=False)
 run(compose+['down'],out/'compose-down.log',check=False)
 try:cg.rmdir()
 except OSError:pass
