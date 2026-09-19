#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Read-only MySQL/HTTP assertions against the marked physical QCT test cluster.

Does not inject faults, alter configuration, or label a successful query as proof
of disk health. The caller selects the expectation after verifying the fault and
execution path. Full wire bodies are represented by a digest and bounded samples.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import http.client
import json
import re
import time

import benchmark
import runtime


WARNING_NAME = "SR_QUERY_PARTIAL_RESULT"
HEALTHY_SQL = "select k, message from qct_faults.healthy order by k"


def checked_select(sql):
    if not re.match(r"(?is)^\s*select\b", sql) or ";" in sql:
        raise ValueError("Probe accepts a single SELECT only")
    if re.search(r"(?is)\binto\s+outfile\b", sql):
        raise ValueError("Probe does not export files")
    return sql


def check_warning(count, warnings, partial):
    if partial:
        assert count == 1, ("Expected one final-packet warning", count)
        assert len(warnings) == 1, warnings
        level, code, message = warnings[0]
        assert level == "Warning" and int(code) == 9000, warnings
        assert WARNING_NAME in message and "query_id=" in message, warnings
    else:
        assert count == 0 and not warnings, (count, warnings)


def check_http(records, status, complete, expectation, raw=False, enabled=True):
    trailers = [record for record in records if "statistics" in record]
    if expectation == "strict":
        assert not trailers, "Strict error must not produce a success trailer"
        assert status >= 400 or not complete, "HTTP 200 with a complete stream is not a strict failure"
        if status >= 400:
            message = json.dumps(records).lower()
            assert any(marker in message for marker in ("corruption", "bad page", "bad segment")), \
                "HTTP error is not evidence of physical corruption: " + message
        # A truncated HTTP 200 intentionally contains no error frame. Correlate its
        # query-id header with the BE corruption log before accepting the fault case.
        return
    assert status == 200 and complete, (status, complete)
    if raw:
        assert not trailers and all("partial_result" not in record for record in records)
        return
    assert len(trailers) == 1 and records[-1] is trailers[0], "Missing or non-final statistics"
    trailer = trailers[0]
    if not enabled:
        assert "partial_result" not in trailer and "warnings" not in trailer, trailer
        return
    partial = expectation == "partial"
    assert trailer["partial_result"] is partial, trailer
    warnings = trailer["warnings"]
    if partial:
        assert len(warnings) == 1 and warnings[0]["code"] == WARNING_NAME, warnings
    else:
        assert not warnings, warnings


def row_summary(rows):
    digest = hashlib.sha256()
    count, sample = 0, []
    for row in rows:
        digest.update(json.dumps(row, default=str, separators=(",", ":")).encode() + b"\n")
        if count < 16:
            sample.append(row)
        count += 1
    return {"rows": count, "sample": sample, "ordered_rows_sha256": digest.hexdigest()}


def variables(args):
    # Test settings, not product policy. Keep shared splitting explicitly selectable.
    return {"enable_query_cache": str(args.cache).lower(), "enable_short_circuit": "false",
            "enable_global_runtime_filter": "false", "pipeline_dop": "2",
            "enable_profile": str(args.profile).lower(), "pipeline_profile_level": "2",
            "enable_tablet_internal_parallel": str(args.split).lower(),
            "tablet_internal_parallel_mode": "force_split" if args.split else "auto"}


def mysql_query(connection, sql, expectation):
    import pymysql
    record = {"sql": sql, "expectation": expectation}
    started = time.perf_counter()
    with connection.cursor() as cursor:
        try:
            cursor.execute(sql)
            record["columns"] = [column[0] for column in cursor.description]
            record.update(row_summary(cursor))
            record["warning_count"] = cursor.warning_count
        except pymysql.MySQLError as error:
            record.update(error_code=error.args[0], error=str(error))
            if expectation != "strict":
                raise
            assert any(marker in str(error).lower() for marker in
                       ("corruption", "bad page", "bad segment")), error
        else:
            assert expectation != "strict", "Expected physical corruption error"
        record["elapsed_ms"] = (time.perf_counter() - started) * 1000
        cursor.execute("show warnings")
        record["warnings"] = list(cursor.fetchall())
        cursor.execute("show warnings")
        assert record["warnings"] == list(cursor.fetchall()), "SHOW WARNINGS consumed the diagnostic"
        if expectation != "strict":
            check_warning(record["warning_count"], record["warnings"], expectation == "partial")
        else:
            assert not any(WARNING_NAME in str(row) for row in record["warnings"])
        cursor.execute("show errors")
        assert not any(WARNING_NAME in str(row) for row in cursor.fetchall())
    return record


def http_query(connection, sql, expectation, settings, raw, enabled):
    body = json.dumps({"query": sql, "onlyOutputResultRaw": raw, "sessionVariables": settings})
    started = time.perf_counter()
    connection.request("POST", "/api/v1/catalogs/default_catalog/sql", body,
                       {"Authorization": "Basic cm9vdDo=", "Content-Type": "application/json",
                        "Connection": "keep-alive"})
    response = connection.getresponse()
    record = {"sql": sql, "expectation": expectation, "http_status": response.status,
              "headers": dict(response.getheaders()), "complete": True}
    try:
        payload = response.read()
    except http.client.IncompleteRead as error:
        payload = error.partial
        record.update(complete=False, error=str(error))
    record["elapsed_ms"] = (time.perf_counter() - started) * 1000
    record["body_sha256"] = hashlib.sha256(payload).hexdigest()
    records = [json.loads(line) for line in payload.splitlines() if line.strip()]
    record["control_records"] = [item for item in records if "data" not in item]
    record.update(row_summary(item["data"] for item in records if "data" in item))
    check_http(records, response.status, record["complete"], expectation, raw, enabled)
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("protocol", choices=("mysql", "http"))
    parser.add_argument("expectation", choices=("healthy", "partial", "strict"))
    parser.add_argument("sql", type=checked_select)
    parser.add_argument("--rows", type=int)
    parser.add_argument("--cache", action="store_true")
    parser.add_argument("--split", action="store_true")
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--raw", action="store_true")
    parser.add_argument("--reuse-healthy", action="store_true")
    args = parser.parse_args()
    if args.raw and (args.protocol != "http" or args.reuse_healthy):
        parser.error("Raw HTTP uses a separate connection; do not reuse its baseline-sticky raw flag")
    if args.expectation == "strict" and args.reuse_healthy:
        parser.error("Strict streaming failure may close the connection; use a new healthy probe")
    root = runtime.checked_root()
    selection = json.loads((root / "selection.json").read_text())
    benchmark.validate_variant(selection["variant"])
    evidence = {"utc": datetime.now(timezone.utc).isoformat(), "runtime": selection,
                "protocol": args.protocol, "settings": variables(args), "queries": []}
    try:
        if args.protocol == "mysql":
            with benchmark.connect() as connection:
                with connection.cursor() as cursor:
                    for name, value in variables(args).items():
                        cursor.execute("set " + name + " = %s", (value,))
                    cursor.execute("explain " + args.sql)
                    evidence["plan"] = "\n".join(row[0] for row in cursor.fetchall())
                evidence["queries"].append(mysql_query(connection, args.sql, args.expectation))
                if args.reuse_healthy:
                    evidence["queries"].append(mysql_query(connection, HEALTHY_SQL, "healthy"))
        else:
            connection = http.client.HTTPConnection("127.0.0.1", 19300, timeout=60)
            enabled = selection["variant"] == "C-on" and not args.raw
            try:
                evidence["queries"].append(http_query(connection, args.sql, args.expectation,
                                                       variables(args), args.raw, enabled))
                if args.reuse_healthy:
                    evidence["queries"].append(http_query(connection, HEALTHY_SQL, "healthy",
                                                           variables(args), False, enabled))
                    ids = [[record["connectionId"] for record in query["control_records"]
                            if "connectionId" in record] for query in evidence["queries"]]
                    assert len(ids[0]) == 1 and ids[0] == ids[1], "Connection was not reused"
            finally:
                connection.close()
        if args.rows is not None:
            assert evidence["queries"][0]["rows"] == args.rows, evidence["queries"][0]
        evidence["passed"] = True
    except BaseException as error:
        evidence.update(passed=False, failure=repr(error))
        raise
    finally:
        print(json.dumps(evidence, indent=2, default=str), flush=True)


if __name__ == "__main__":
    main()
