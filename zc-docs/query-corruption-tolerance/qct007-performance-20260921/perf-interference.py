#!/usr/bin/env python3
import json
from datetime import datetime, timezone
from pathlib import Path
import sys

sys.path.insert(0, '/query-corruption-workspace/src/tools/query_corruption')
import matrix
import runtime
import supplement

matrix.ensure_idle_builds()
matrix.ensure_restored()
assert all(runtime.live_record(n) is None for n in runtime.NODES), 'Finish the healthy matrices first'
source = matrix.LOG_ROOT / 'one-tablet-pages.json'
segment, page = supplement.checked_snapshot(source, 10155)
destination = matrix.LOG_ROOT / 'qct007-20260921-interference-warm-r1'
destination.mkdir(exist_ok=False)
try:
    runtime.prepare('C-on', False)
    for node in runtime.NODES:
        runtime.start(node)
    readiness = matrix.wait_ready('C-on')
    supplement.save(destination, 'build', matrix.manifest('C-on', readiness))
    with supplement.configured() as connection, connection.cursor() as cursor:
        cursor.execute('show tablet from qct_faults.one_tablet')
        supplement.confirm_tablet(list(cursor.fetchall()), 10155)
    supplement.interference_phase(destination, 'control-before', False, 1000, 2000)
    print('control-before completed', flush=True)
    with supplement.fault(segment, 10155, 'page', 0) as manifest:
        supplement.save(destination, 'fault', {'manifest': str(manifest)})
        supplement.interference_phase(destination, 'fault-concurrent', True, 1000, 2000)
        print('fault-concurrent completed', flush=True)
    supplement.interference_phase(destination, 'control-after', False, 1000, 2000)
    print('control-after completed', flush=True)
    supplement.run_query(destination, 'restored', 'select k,message from qct_faults.one_tablet order by k', 32768, 'healthy')
    matrix.ensure_restored()
    supplement.save(destination, 'completed', {'passed': True, 'utc': datetime.now(timezone.utc).isoformat()})
finally:
    matrix.stop_all()
