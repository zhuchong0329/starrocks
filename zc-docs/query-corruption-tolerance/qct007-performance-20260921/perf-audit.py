#!/usr/bin/env python3
import collections
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, '/query-corruption-workspace/src/tools/query_corruption')
import matrix
import runtime
import summarize

root = matrix.LOG_ROOT
series = [root / ('qct007-20260921-' + s) for s in (
    'process-restart', 'warm-page-r1', 'warm-page-r2', 'focused-r1', 'focused-r2',
    'page-cache-off-r1', 'page-cache-off-r2')]
extended = '--include-rechecks' in sys.argv
if extended:
    series += [root / ('qct007-20260921-page-cache-off-' + s) for s in ('r3', 'r4')]
samples = 0
files = 0
placements = collections.defaultdict(set)
artifacts = collections.defaultdict(set)
plans = collections.defaultdict(set)
oom = set()
versions = collections.defaultdict(set)
for directory in series:
    for path in sorted(directory.glob('*.jsonl')):
        metadata, values, final = summarize.load_run(path)
        samples += len(values)
        files += 1
        versions[metadata['variant']].add(str(metadata.get('version')))
        for case, plan in metadata['plans'].items():
            key = (metadata['protocol'], metadata['settings']['cache'], metadata['settings']['dop'], case)
            plans[key].add(hashlib.sha256(plan.encode()).hexdigest())
        for side in (metadata['server_resources_before'], final['server_resources_after']):
            counters = dict(line.split() for line in side['cgroup']['memory.events'].splitlines())
            oom.add(int(counters['oom_kill']))
    for path in sorted(directory.glob('*-build.json')):
        manifest = json.loads(path.read_text())
        variant = manifest['selection']['variant']
        expected = matrix.BASELINE if variant == 'A-baseline' else '46ca14950dc76004c5df355ce2c975a77c50041a'
        assert manifest['source_revision'] == expected
        assert expected in manifest['be_version']
        for name, rows in manifest['readiness']['fixture_tablets'].items():
            placement = sorted((row['TabletId'], row['BackendId'], row['Version']) for row in rows)
            placements[name].add(json.dumps(placement))
        for name, info in manifest['files'].items():
            if name.endswith(('starrocks_be', 'starrocks-fe.jar')):
                artifacts[(variant, Path(name).name)].add(info['sha256'])
assert samples == (361512 if extended else 343512), samples
assert files == (123 if extended else 111), files
assert all(len(p) == 1 for p in placements.values())
assert all(len(p) == 1 for p in plans.values())
assert all(len(a) == 1 for a in artifacts.values())
for name in ('starrocks_be', 'starrocks-fe.jar'):
    assert artifacts[('B-off', name)] == artifacts[('C-on', name)]
matrix.ensure_idle_builds()
matrix.ensure_restored()
assert all(runtime.live_record(n) is None for n in runtime.NODES)
commands = []
for source in ('baseline-src', 'src'):
    directory = runtime.WORKSPACE / source
    entries = json.loads((directory / 'be/build_Release/compile_commands.json').read_text())
    commands.append(sorted((e['file'].replace(str(directory), '<SRC>'),
                            e['command'].replace(str(directory), '<SRC>')) for e in entries))
assert commands[0] == commands[1], 'Production compiler commands differ'
print(json.dumps({'samples': samples, 'completed_files': files, 'plan_groups': len(plans),
                  'normalized_compile_commands_equal': True, 'compile_units': len(commands[0]),
                  'all_plans_identical': True, 'all_tablet_placements_versions_identical': True,
                  'oom_kill_counts': sorted(oom), 'artifacts': {str(k): sorted(v) for k,v in artifacts.items()},
                  'faults_restored': True, 'all_qct_nodes_stopped': True}, indent=2))
