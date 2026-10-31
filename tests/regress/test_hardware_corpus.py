#!/usr/bin/env python3
"""Planner/evidence regressions; these tests do not claim hardware execution."""
import importlib.util
import argparse
import os
import signal
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("hardware_corpus", ROOT / "scripts/hardware-corpus.py")
corpus = importlib.util.module_from_spec(spec)
spec.loader.exec_module(corpus)

class HardwareCorpusTests(unittest.TestCase):
    def test_workspace_reuse_follows_only_recorded_continuation_ancestry(self):
        with tempfile.TemporaryDirectory(prefix="remex-workspace-lineage-") as temp:
            root = Path(temp)
            first, second, third = [root / name for name in ("first", "second", "third")]
            for path in (first, second, third):
                path.mkdir()
            args = argparse.Namespace(reuse_resume_workspace=True, resume_from=second)
            (second / "resume-provenance.json").write_text(json.dumps({"source":str(first)}))
            proof = second / "workspace-provenance.json"
            proof.write_text(json.dumps({"worktree":str(first / "worktree"), "bazel_state":str(first / "bazel-state")}))
            self.assertEqual(corpus.workspace_paths(args, third), (first / "worktree", first / "bazel-state"))
            proof.write_text(json.dumps({"worktree":str(root / "outside/worktree"), "bazel_state":str(root / "outside/bazel-state")}))
            with self.assertRaisesRegex(ValueError,"outside recorded"):
                corpus.workspace_paths(args, third)
            proof.unlink()
            (second / "bazel-state").symlink_to(root / "outside/bazel-state")
            with self.assertRaisesRegex(ValueError,"outside recorded"):
                corpus.workspace_paths(args, third)
            (first / "resume-provenance.json").write_text(json.dumps({"source":str(second)}))
            with self.assertRaisesRegex(ValueError,"cyclic"):
                corpus.workspace_paths(args, third)

    def test_gate_boundary_and_sparse_ranges(self):
        self.assertEqual(corpus.parse_indices("71-72,80,79", 96), [71,72,79,80])
        self.assertEqual(len(corpus.parse_indices("all", 96)), 96)
        for text in ("96", "72-71", "-1"):
            with self.assertRaises(ValueError):
                corpus.parse_indices(text,96)
        observed = sum(corpus.seed_gate(seed) for seed in range(4096))
        self.assertGreater(observed, 800)
        self.assertLess(observed, 1250)
        trigger = next(seed for seed in range(32) if corpus.seed_gate(seed))
        quiet = next(seed for seed in range(32) if not corpus.seed_gate(seed))
        scenario = {"culprit_index":72,"seed_gate":"splitmix64(seed) % 4 == 0"}
        self.assertEqual(corpus.expected_category(scenario,71,trigger),"PASS")
        self.assertEqual(corpus.expected_category(scenario,72,trigger),"TEST_FAILURE")
        self.assertEqual(corpus.expected_category(scenario,72,quiet),"PASS")

    def test_selection_miss_differs_from_replay_disagreement(self):
        passed = {"category":"PASS"}
        failed = {"category":"TEST_FAILURE","signature":"cuda_lost_update"}
        self.assertEqual(corpus.compare_observations(failed,passed,False),"MISSED_FAILURE")
        self.assertEqual(corpus.compare_observations(failed,passed,True),"REPLAY_DISAGREEMENT")
        self.assertEqual(corpus.compare_observations(failed,None,False),"INCONCLUSIVE")
        self.assertEqual(corpus.compare_observations(failed,{"category":"INFRASTRUCTURE"},True),"INCONCLUSIVE")
        self.assertEqual(corpus.compare_observations(failed,failed,False),"AGREE")

    def test_ledger_is_scoped_to_exact_invocation(self):
        data = {"schema_version":1,"executions":[{"id":"e1","invocation_ids":["wanted"]},{"id":"e2","invocation_ids":["other"]}],
                "attempts":[{"id":"a1","execution_id":"e1"},{"id":"a2","execution_id":"e2"}],
                "events":[{"execution_id":"e1"},{"execution_id":"e2"}]}
        result = corpus.scope_ledger(data,"wanted")
        self.assertEqual([a["id"] for a in result["attempts"]],["a1"])
        self.assertEqual(len(result["events"]),1)
        self.assertEqual(corpus.scope_ledger(data,"absent")["attempts"],[])

    def test_compile_attempt_cannot_prove_hardware_test(self):
        digest = {"hash":"a"*64,"size_bytes":17}
        result = {"testActionOutput":[{"name":"test.log","uri":"bytestream://cas/instance/blobs/"+digest["hash"]+"/17"}]}
        execution = {"id":"e", "winner_attempt_id":"a", "do_not_cache":True, "platform":{"pool":"gpu"}}
        # published denotes AC publication, which uncached hardware tests forbid.
        attempt = {"id":"a", "execution_id":"e", "published":False,"worker_platform":{"pool":"gpu"}, "result_digests":[digest]}
        ledger = {"executions":[execution],"attempts":[attempt]}
        self.assertEqual(corpus.hardware_test_attempts(ledger,result,"gpu"),[attempt])
        attempt["result_digests"] = [{"hash":"b"*64,"size_bytes":17}]
        self.assertEqual(corpus.hardware_test_attempts(ledger,result,"gpu"),[])
        attempt["result_digests"] = [digest]
        execution["do_not_cache"] = False
        self.assertEqual(corpus.hardware_test_attempts(ledger,result,"gpu"),[])

    def test_retry_proof_preserves_ambiguous_identical_logs_and_distinct_winners(self):
        with tempfile.TemporaryDirectory(prefix="remex-retry-proof-") as temp:
            root = Path(temp)
            logs = root/"logs"
            logs.mkdir()
            data = b"REMEX_SIGNATURE: rtl_simultaneous_count\n"
            (logs/"test.log").write_bytes(data)
            digest = {"hash":hashlib.sha256(data).hexdigest(), "size_bytes":len(data)}
            uri = "bytestream://cas/blobs/"+digest["hash"]+"/"+str(len(data))
            events = [{"id":{"testResult":{"label":"//flaky:test","attempt":index+1}},
                       "testResult":{"status":"FAILED","executionInfo":{"strategy":"remote"},
                                     "testActionOutput":[{"name":"test.log","uri":uri}]}} for index in range(3)]
            ledger = {"executions":[],"attempts":[]}
            for index in range(3):
                key = str(index)
                ledger["executions"].append({"id":key,"winner_attempt_id":key,"do_not_cache":True,"platform":{"pool":"rtl"}})
                ledger["attempts"].append({"id":key,"execution_id":key,"worker_platform":{"pool":"rtl"},"cgroup_empty":True,"result_digests":[digest]})
            evidence = root/"proof"
            evidence.mkdir()
            proof = corpus.retry_evidence(ledger,events,{"hardware":"rtl"},logs,evidence)
            self.assertEqual(proof["distinct_winner_count"],3)
            self.assertFalse(proof["unique_pairing"])
            self.assertEqual(len(proof["retries"]),3)
            self.assertEqual(corpus.retry_classification(proof,3,"rtl_simultaneous_count")["category"],"TEST_FAILURE")
            proof["retries"][-1]["status"] = "PASSED"
            proof["retries"][-1]["signatures"] = []
            classified = corpus.retry_classification(proof,0,"rtl_simultaneous_count")
            self.assertEqual(classified["category"],"PASS")
            self.assertTrue(classified["flaky_observed"])
            success=root/"success";success.mkdir()
            success_event=json.loads(json.dumps(events[0]));success_event["testResult"]["status"]="PASSED"
            success_attempt=dict(ledger["attempts"][0],failure_category="SUCCESS",status="OK")
            success_proof=corpus.retry_evidence({"executions":ledger["executions"][:1],"attempts":[success_attempt]},[success_event],{"hardware":"rtl"},logs,success)
            self.assertEqual(corpus.retry_classification(success_proof,0,"rtl_simultaneous_count")["category"],"PASS")
            # Three BEP attempts cannot be justified by one winner reused thrice.
            ledger["attempts"] = ledger["attempts"][:1]
            missing = root/"missing"
            missing.mkdir()
            with self.assertRaisesRegex(ValueError,"distinct matching hardware winner count"):
                corpus.retry_evidence(ledger,events,{"hardware":"rtl"},logs,missing)

    def test_inventory_requires_observed_tags_and_complete_universe(self):
        xml = '<query><rule name="//race:race_test"><list name="tags"><string value="remex-hw"/><string value="no-remote-cache"/></list></rule></query>'
        self.assertEqual(corpus.test_inventory(xml,["//race:race_test"])[0]["label"],"//race:race_test")
        with self.assertRaises(ValueError):
            corpus.test_inventory(xml,["//missing:test"])
        with self.assertRaises(ValueError):
            corpus.test_inventory(xml.replace('value="no-remote-cache"','value="manual"'),["//race:race_test"])

    def test_probe_rejects_unknown_revision_before_checkout_or_execution(self):
        with tempfile.TemporaryDirectory(prefix="remex-probe-validation-") as temp:
            state = Path(temp)/"state.json"
            state.write_text(json.dumps({"commits":["a"*40]}))
            with self.assertRaisesRegex(ValueError,"outside the declared corpus"):
                corpus.hardware_probe(state,"b"*40,0,Path(temp)/"evidence")
            with self.assertRaisesRegex(ValueError,"outside the declared corpus"):
                corpus.hardware_probe(state,"a"*40,-1,Path(temp)/"evidence")

    def test_timeout_kills_descendants_before_returning(self):
        with tempfile.TemporaryDirectory(prefix="remex-timeout-test-") as temp:
            marker = Path(temp)/"late-child-write"
            child = "import time; from pathlib import Path; time.sleep(.4); Path("+repr(str(marker))+").write_text('leaked')"
            parent = "import subprocess,sys,time; subprocess.Popen([sys.executable,'-c',"+repr(child)+"]); time.sleep(10)"
            code, output, _ = corpus.command([sys.executable,"-c",parent],Path(temp),timeout=.1)
            self.assertEqual(code,124)
            self.assertIn("command timed out",output)
            time.sleep(.5)
            self.assertFalse(marker.exists(),"timeout left an owned descendant running")

    def test_outer_timeout_kills_nested_new_sessions_and_orphans(self):
        with tempfile.TemporaryDirectory(prefix="remex-nested-timeout-") as temp:
            marker = Path(temp)/"escaped-write"
            child = "import time; from pathlib import Path; time.sleep(.8); Path("+repr(str(marker))+").write_text('leaked')"
            # Both the probe's command() and its child start independent sessions.
            escaped = "import subprocess,sys; subprocess.Popen([sys.executable,'-c',"+repr(child)+"],start_new_session=True)"
            inner = Path(temp)/"inner.py"
            inner.write_text("import importlib.util,sys\nfrom pathlib import Path\ns=importlib.util.spec_from_file_location('corpus',"+
                             repr(str(ROOT/"scripts/hardware-corpus.py"))+");m=importlib.util.module_from_spec(s);s.loader.exec_module(m)\n"+
                             "m.command([sys.executable,'-c',"+repr(escaped)+"],Path("+repr(temp)+"),timeout=10)\n")
            code, output, elapsed = corpus.command([sys.executable,inner],Path(temp),timeout=.3)
            self.assertEqual(code,124)
            self.assertLess(elapsed,2)
            time.sleep(.9)
            self.assertFalse(marker.exists(),"outer timeout leaked a detached nested probe descendant")

    def test_driver_death_cleans_detached_writing_descendant(self):
        with tempfile.TemporaryDirectory(prefix="remex-owner-death-") as temp:
            root=Path(temp);ready=root/"ready";late=root/"late"
            writer="import time,sys;from pathlib import Path;Path("+repr(str(ready))+").write_text('ready');\nfor i in range(100):\n print('output'*100,flush=True);time.sleep(.01)\nPath("+repr(str(late))+").write_text('leaked')"
            detached="import subprocess,sys;subprocess.Popen([sys.executable,'-c',"+repr(writer)+"],start_new_session=True)"
            owner=root/"owner.py"
            owner.write_text("import importlib.util,sys\nfrom pathlib import Path\ns=importlib.util.spec_from_file_location('c',"+repr(str(ROOT/"scripts/hardware-corpus.py"))+");m=importlib.util.module_from_spec(s);s.loader.exec_module(m)\nm.command([sys.executable,'-c',"+repr(detached)+"],Path("+repr(temp)+"),timeout=10)\n")
            process=subprocess.Popen([sys.executable,owner],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            try:
                deadline=time.monotonic()+5
                while not ready.exists() and time.monotonic()<deadline:
                    time.sleep(.01)
                self.assertTrue(ready.exists())
                process.kill();process.wait(timeout=5)
                time.sleep(1.2)
                self.assertFalse(late.exists(),"dead driver's broken stdout pipe bypassed descendant cleanup")
            finally:
                if process.poll() is None:
                    process.kill();process.wait()

    def test_selector_fingerprint_detects_same_size_edits_and_symlink_target(self):
        with tempfile.TemporaryDirectory(prefix="remex-selector-input-") as temp:
            root=Path(temp)
            file=root/"toolchain"
            file.write_text("aaaa")
            link=root/"compiler"
            link.symlink_to(file)
            cache=corpus.FileFingerprint()
            before=corpus.json_digest(cache.snapshot([root]))
            stamp=file.stat().st_mtime_ns
            file.write_text("bbbb")
            os.utime(file,ns=(stamp,stamp))
            after=corpus.json_digest(cache.snapshot([root]))
            self.assertNotEqual(before,after,"same-size input edits must invalidate selector results")
            file2=root/"other"
            file2.write_text("bbbb")
            before=corpus.json_digest(cache.snapshot([link]))
            link.unlink();link.symlink_to(file2)
            self.assertNotEqual(before,corpus.json_digest(cache.snapshot([link])))

    def test_external_input_fingerprint_excludes_only_verified_main_output_cycle(self):
        with tempfile.TemporaryDirectory(prefix="remex-external-fingerprint-") as temp:
            root=Path(temp);workspace=root/"workspace";workspace.mkdir();external=root/"external";external.mkdir()
            (external/"_main").symlink_to(workspace,target_is_directory=True)
            dependency=external/"dependency";dependency.mkdir();(dependency/"BUILD.bazel").write_text("source")
            output=root/"bazel-output";output.mkdir();(output/"test.cache_status").write_text("first")
            (workspace/"bazel-out").symlink_to(output,target_is_directory=True)
            inputs=corpus.external_selector_inputs(external,workspace)
            fingerprints=corpus.FileFingerprint();before=corpus.json_digest(fingerprints.snapshot(inputs))
            (output/"test.cache_status").write_text("second")
            self.assertEqual(before,corpus.json_digest(fingerprints.snapshot(inputs)))
            (dependency/"BUILD.bazel").write_text("changed")
            self.assertNotEqual(before,corpus.json_digest(fingerprints.snapshot(inputs)))
            (external/"_main").unlink();(external/"_main").symlink_to(output,target_is_directory=True)
            with self.assertRaisesRegex(ValueError,"unexpected Bazel"):
                corpus.external_selector_inputs(external,workspace)

    def test_selector_cache_rejects_hidden_tracked_edits_and_external_source_links(self):
        with tempfile.TemporaryDirectory(prefix="remex-source-identity-") as temp:
            root=Path(temp);workspace=root/"cuda";workspace.mkdir();source=workspace/"test.cc";source.write_text("original")
            def git(*args):
                return subprocess.run(["git","-C",str(root),*args],check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            git("init","-q");git("config","user.name","test");git("config","user.email","test@example.invalid")
            git("add",".");git("commit","-qm","original")
            self.assertIn(source,corpus.checked_workspace_inputs(None,root,"cuda"))
            for flag in ("assume-unchanged","skip-worktree"):
                git("update-index","--"+flag,"cuda/test.cc");source.write_text("modified")
                self.assertEqual(git("status","--porcelain").stdout,b"")
                with self.assertRaisesRegex(ValueError,"index flags"):
                    corpus.checked_workspace_inputs(None,root,"cuda")
                source.write_text("original");git("update-index","--no-"+flag,"cuda/test.cc")
            target=root/"outside";target.write_text("outside")
            (workspace/"linked.cc").symlink_to(target)
            git("add","cuda/linked.cc");git("commit","-qm","linked")
            with self.assertRaisesRegex(ValueError,"without symlinks/gitlinks"):
                corpus.checked_workspace_inputs(None,root,"cuda")

    def test_resume_keeps_infra_rejects_changed_flags_and_imports_only_complete_revisions(self):
        with tempfile.TemporaryDirectory(prefix="remex-resume-proof-") as temp:
            root=Path(temp);source=root/"original";source.mkdir()
            args=argparse.Namespace(resume_from=source,resume_legacy=True,bazel_flag=[],cuda_flag=[],rtl_flag=[],
                                   executor="grpc://executor",cache="grpc://cache",instance="test",_run_contract={"version":"one"})
            scenario={"workspace":"cuda","target":"//race:test","hardware":"gpu","culprit_index":1,"seed_gate":"gate"}
            oracle={"commits":["a"*40,"b"*40],"scenarios":{"cuda_race":scenario}}
            rows=[]
            for index,seed in ((0,0),(0,6),(1,0)):
                pair={}
                for arm in ("oracle","selected"):
                    path=source/("revision-%02d"%index)/arm/"cuda_race"/str(seed);path.mkdir(parents=True)
                    value={"category":"INFRASTRUCTURE","signature":"","index":index,"revision":oracle["commits"][index],
                           "seed":seed,"scenario":"cuda_race","arm":arm,"invocation":str(index)+arm+str(seed),
                           "evidence":str(path),"expected_category":corpus.expected_category(scenario,index,seed),"matches_planted_oracle":False}
                    corpus.save(path/"observation.json",value)
                    corpus.save(path/"probe.json",{**{key:value[key] for key in ("revision","index","seed","scenario","arm","invocation")},"command":[str(x) for x in corpus.test_command(args,"wrapper",scenario,seed,value["invocation"],path)]})
                    (path/"bazel.log").write_text("infrastructure unavailable")
                    pair[arm]=value
                rows.append(dict(index=index,scenario="cuda_race",seed=seed,selected_executed=True,verdict="INCONCLUSIVE",**pair))
                directory=source/("revision-%02d"%index)/"selection"
                corpus.save(directory/"cuda-selection.json",{"targets":["//race:test"]})
                corpus.save(directory/"environment.json",{"image":"native","driver":"595"})
            (source/"comparisons.jsonl").write_text("".join(json.dumps(row)+"\n" for row in rows))
            continuation=root/"continuation";continuation.mkdir()
            result=corpus.resume_checkpoint(args,oracle,[0,1],["cuda_race"],[0,6],continuation)
            self.assertEqual(result[-1],[0])
            self.assertEqual(len(result[0]),2)
            self.assertTrue(all(row["verdict"]=="INCONCLUSIVE" for row in result[0]))
            self.assertEqual(json.loads((continuation/"resume-provenance.json").read_text())["incomplete_or_out_of_scope_rows"],1)
            args._seen_invocations=set()
            args.bazel_flag=["--compilation_mode=opt"]
            with self.assertRaisesRegex(ValueError,"flags differ"):
                corpus.resume_checkpoint(args,oracle,[0,1],["cuda_race"],[0,6],root/"changed")
            args.bazel_flag=[]
            args._seen_invocations=set()
            identity_path=Path(rows[0]["oracle"]["evidence"])/"probe.json"
            identity=json.loads(identity_path.read_text())
            changed=dict(identity,revision="f"*40)
            corpus.save(identity_path,changed)
            with self.assertRaisesRegex(ValueError,"raw probe identity differs"):
                corpus.resume_checkpoint(args,oracle,[0,1],["cuda_race"],[0,6],root/"wrong-revision")
            corpus.save(identity_path,identity)
            args._seen_invocations={rows[0]["oracle"]["invocation"]}
            with self.assertRaisesRegex(ValueError,"reuse an invocation"):
                corpus.resume_checkpoint(args,oracle,[0,1],["cuda_race"],[0,6],root/"same-invocation")
            args._seen_invocations=set()
            # A second resume checks source raw hashes, not just the JSONL hash.
            (Path(rows[0]["oracle"]["evidence"])/"bazel.log").write_text("tampered")
            args.resume_from=continuation
            with self.assertRaisesRegex(ValueError,"raw evidence changed"):
                corpus.resume_checkpoint(args,oracle,[0,1],["cuda_race"],[0,6],root/"second")

    def test_generated_history_has_independent_real_hardware_boundaries(self):
        with tempfile.TemporaryDirectory(prefix="remex-history-test-") as temp:
            target = Path(temp)/"new-history"
            subprocess.run([sys.executable,str(ROOT/"scripts/generate-history.py"),"--output",str(target)],check=True,capture_output=True)
            oracle=json.loads((target/"oracle.json").read_text())
            self.assertEqual(oracle["count"],96)
            self.assertEqual(len(set(oracle["commits"])),96)
            self.assertEqual(oracle["scenarios"]["cuda_race"]["culprit_index"],72)
            self.assertEqual(oracle["scenarios"]["rtl_flaky"]["culprit_index"],80)
            def content(index,path):
                return subprocess.check_output(["git","-C",str(target),"show",oracle["commits"][index]+":"+path],text=True)
            self.assertIn("kPlantedRace = false",content(71,"cuda/race/race_test.cu"))
            self.assertIn("kPlantedRace = true",content(72,"cuda/race/race_test.cu"))
            self.assertIn("PLANTED_BUG = 1'b0",content(79,"rtl/flaky/fifo_flaky.sv"))
            self.assertIn("PLANTED_BUG = 1'b1",content(80,"rtl/flaky/fifo_flaky.sv"))
            self.assertIn("atomicAdd",content(72,"cuda/race/race_test.cu"))
            self.assertIn("__syncthreads",content(72,"cuda/race/race_test.cu"))
            self.assertIn("Vfifo_flaky",content(80,"rtl/flaky/fifo_flaky_test.cc"))
            self.assertIn("if (true",content(72,"flaky.cc"))
            retry=subprocess.run([sys.executable,str(ROOT/"scripts/generate-history.py"),"--output",str(target)],capture_output=True,text=True)
            self.assertNotEqual(retry.returncode,0)
            self.assertIn("Refusing to overwrite",retry.stderr)

if __name__ == "__main__":
    unittest.main()
