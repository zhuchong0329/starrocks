#!/usr/bin/env python3
"""Reproducible StarRocks partition-granularity compaction benchmark."""

from __future__ import annotations

import argparse
import base64
import csv
import datetime as dt
import ipaddress
import json
import math
import os
import random
import re
import signal
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib
from collections import deque
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


SCRIPT_DIR = Path(__file__).resolve().parent
SCHEMA_PATH = SCRIPT_DIR / "schema.sql"
TABLE_FINE = "http_log_5m_x7"
TABLE_COARSE = "http_log_35m_x1"
ASIA_SHANGHAI = dt.timezone(dt.timedelta(hours=8), name="Asia/Shanghai")
BASE_EVENT_TIME = dt.datetime(2026, 8, 28, 6, 0, 0, tzinfo=ASIA_SHANGHAI)
# macOS can expose system-wide proxies to urllib even when no proxy variables are
# present in the shell. Benchmark endpoints are normally private/local, so bypass
# proxies explicitly for both metrics and Stream Load traffic.
HTTP_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
METRIC_FIELDS = [
    "base_compaction_bps",
    "base_compaction_cost_ms",
    "base_compaction_bytes",
    "cumulative_compaction_bytes",
    "base_compaction_deltas",
    "cumulative_compaction_deltas",
    "compaction_mem_bytes",
    "cumulative_compaction_bps",
    "cumulative_compaction_cost_ms",
    "running_base",
    "running_cumulative",
    "wait_base",
    "wait_cumulative",
    "tablet_base_max_score",
    "tablet_cumulative_max_score",
    "process_mem_bytes",
    "disk_read_bytes",
    "disk_write_bytes",
    "disk_io_time_ms",
    "disk_avail_bytes",
    "disk_data_used_bytes",
    "max_disk_io_util_percent",
    "rowset_count",
    "cpu_idle",
    "cpu_iowait",
    "cpu_user",
    "cpu_system",
    "cpu_nice",
    "cpu_irq",
    "cpu_soft_irq",
    "cpu_steal",
]


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def percentile(values: Sequence[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(p * len(ordered)) - 1))
    return float(ordered[index])


class MysqlClient:
    def __init__(self, host: str, port: int, user: str, password: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    def _command(self) -> List[str]:
        return [
            "mysql",
            "--connect-timeout=10",
            f"-h{self.host}",
            f"-P{self.port}",
            f"-u{self.user}",
            "--batch",
            "--raw",
        ]

    def _env(self) -> Dict[str, str]:
        env = os.environ.copy()
        if self.password:
            env["MYSQL_PWD"] = self.password
        return env

    def execute(self, sql: str) -> str:
        proc = subprocess.run(
            self._command() + ["--execute", sql],
            env=self._env(),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if proc.returncode != 0:
            raise RuntimeError(f"mysql failed: {proc.stderr.strip()}\nSQL: {sql}")
        return proc.stdout

    def execute_script(self, path: Path) -> None:
        with path.open("rb") as handle:
            proc = subprocess.run(
                self._command(),
                env=self._env(),
                stdin=handle,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        if proc.returncode != 0:
            raise RuntimeError(f"mysql script failed: {proc.stderr.decode(errors='replace').strip()}")

    def query_rows(self, sql: str) -> List[Dict[str, str]]:
        output = self.execute(sql)
        lines = output.splitlines()
        if not lines:
            return []
        reader = csv.DictReader(lines, delimiter="\t")
        return [dict(row) for row in reader]


def parse_prometheus(text: str) -> Dict[str, float]:
    parsed: Dict[str, float] = {}
    cpu_modes: Dict[str, float] = {}
    disk_sums = {"read": 0.0, "write": 0.0, "io_time": 0.0, "used": 0.0}
    disk_avail: List[float] = []
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        match = re.match(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([-+0-9.eE]+)$', line)
        if not match:
            continue
        name, labels_text, value_text = match.groups()
        try:
            value = float(value_text)
        except ValueError:
            continue
        labels = dict(re.findall(r'(\w+)="([^"]*)"', labels_text or ""))
        if name == "starrocks_be_cpu":
            cpu_modes[labels.get("mode", "unknown")] = value
        elif name == "starrocks_be_disk_bytes_read":
            disk_sums["read"] += value
        elif name == "starrocks_be_disk_bytes_written":
            disk_sums["write"] += value
        elif name == "starrocks_be_disk_io_time_ms":
            disk_sums["io_time"] += value
        elif name == "starrocks_be_disks_avail_capacity":
            disk_avail.append(value)
        elif name == "starrocks_be_disks_data_used_capacity":
            disk_sums["used"] += value
        elif name == "starrocks_be_compaction_bytes_total":
            parsed[f"{labels.get('type')}_compaction_bytes"] = value
        elif name == "starrocks_be_compaction_deltas_total":
            parsed[f"{labels.get('type')}_compaction_deltas"] = value
        else:
            direct = {
                "starrocks_be_base_compaction_task_byte_per_second": "base_compaction_bps",
                "starrocks_be_base_compaction_task_cost_time_ms": "base_compaction_cost_ms",
                "starrocks_be_compaction_mem_bytes": "compaction_mem_bytes",
                "starrocks_be_cumulative_compaction_task_byte_per_second": "cumulative_compaction_bps",
                "starrocks_be_cumulative_compaction_task_cost_time_ms": "cumulative_compaction_cost_ms",
                "starrocks_be_running_base_compaction_task_num": "running_base",
                "starrocks_be_running_cumulative_compaction_task_num": "running_cumulative",
                "starrocks_be_wait_base_compaction_task_num": "wait_base",
                "starrocks_be_wait_cumulative_compaction_task_num": "wait_cumulative",
                "starrocks_be_tablet_base_max_compaction_score": "tablet_base_max_score",
                "starrocks_be_tablet_cumulative_max_compaction_score": "tablet_cumulative_max_score",
                "starrocks_be_process_mem_bytes": "process_mem_bytes",
                "starrocks_be_max_disk_io_util_percent": "max_disk_io_util_percent",
                "starrocks_be_rowset_count_generated_and_in_use": "rowset_count",
            }
            if name in direct:
                parsed[direct[name]] = value
    parsed["disk_read_bytes"] = disk_sums["read"]
    parsed["disk_write_bytes"] = disk_sums["write"]
    parsed["disk_io_time_ms"] = disk_sums["io_time"]
    parsed["disk_avail_bytes"] = min(disk_avail) if disk_avail else 0.0
    parsed["disk_data_used_bytes"] = disk_sums["used"]
    for mode in ("idle", "iowait", "user", "system", "nice", "irq", "soft_irq", "steal"):
        parsed[f"cpu_{mode}"] = cpu_modes.get(mode, 0.0)
    return {field: parsed.get(field, 0.0) for field in METRIC_FIELDS}


class MetricsSampler:
    def __init__(
        self,
        mysql: MysqlClient,
        metrics_url: str,
        output_dir: Path,
        stage: str,
        table: Optional[str],
        min_disk_free_bytes: int,
        interval: float = 1.0,
        tablet_interval: float = 5.0,
    ):
        self.mysql = mysql
        self.metrics_url = metrics_url
        self.output_dir = output_dir
        self.stage = stage
        self.table = table
        self.min_disk_free_bytes = min_disk_free_bytes
        self.interval = interval
        self.tablet_interval = tablet_interval
        self.stop_event = threading.Event()
        self.fatal_event = threading.Event()
        self.fatal_reason = ""
        self.thread: Optional[threading.Thread] = None
        self.lock = threading.Lock()
        self.latest_metrics: Dict[str, float] = {}
        self.latest_tablets: Dict[str, float] = {}
        self.tablet_ids: List[int] = []
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def start(self) -> None:
        if self.table:
            rows = self.mysql.query_rows(f"SHOW TABLET FROM zc_test.{self.table}")
            self.tablet_ids = sorted(int(row["TabletId"]) for row in rows)
            if not self.tablet_ids:
                raise RuntimeError(f"no tablets found for zc_test.{self.table}")
        self.thread = threading.Thread(target=self._run, name=f"metrics-{self.stage}", daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=15)

    def _fetch_metrics(self) -> Dict[str, float]:
        with HTTP_OPENER.open(self.metrics_url, timeout=10) as response:
            return parse_prometheus(response.read().decode("utf-8", errors="replace"))

    def snapshot_tablets(self) -> Dict[str, float]:
        if not self.tablet_ids:
            return {}
        ids = ",".join(str(item) for item in self.tablet_ids)
        sql = (
            "SELECT COALESCE(SUM(NUM_VERSION),0) total_versions, "
            "COALESCE(MAX(NUM_VERSION),0) max_versions, "
            "COALESCE(SUM(NUM_ROWSET),0) total_rowsets, "
            "COALESCE(MAX(NUM_ROWSET),0) max_rowsets, "
            "COALESCE(SUM(NUM_SEGMENT),0) total_segments, "
            "COALESCE(MAX(NUM_SEGMENT),0) max_segments, "
            "COALESCE(SUM(DATA_SIZE),0) data_size, "
            "COALESCE(SUM(NUM_ROW),0) num_rows "
            f"FROM information_schema.be_tablets WHERE TABLET_ID IN ({ids})"
        )
        rows = self.mysql.query_rows(sql)
        if not rows:
            return {}
        return {key: float(value or 0) for key, value in rows[0].items()}

    def _run(self) -> None:
        metrics_path = self.output_dir / "metrics.csv"
        tablets_path = self.output_dir / "tablets.csv"
        metric_headers = ["epoch", "utc_time", "stage"] + METRIC_FIELDS
        tablet_headers = [
            "epoch",
            "utc_time",
            "stage",
            "total_versions",
            "max_versions",
            "total_rowsets",
            "max_rowsets",
            "total_segments",
            "max_segments",
            "data_size",
            "num_rows",
        ]
        next_tablet = 0.0
        with metrics_path.open("w", newline="") as metrics_file, tablets_path.open("w", newline="") as tablet_file:
            metrics_writer = csv.DictWriter(metrics_file, fieldnames=metric_headers)
            tablet_writer = csv.DictWriter(tablet_file, fieldnames=tablet_headers)
            metrics_writer.writeheader()
            tablet_writer.writeheader()
            while not self.stop_event.is_set():
                started = time.monotonic()
                epoch = time.time()
                try:
                    metrics = self._fetch_metrics()
                    metrics_writer.writerow({"epoch": epoch, "utc_time": utc_now(), "stage": self.stage, **metrics})
                    metrics_file.flush()
                    with self.lock:
                        self.latest_metrics = metrics
                    if 0 < metrics["disk_avail_bytes"] < self.min_disk_free_bytes:
                        self.fatal_reason = (
                            f"disk free {metrics['disk_avail_bytes'] / 2**30:.2f} GiB is below "
                            f"limit {self.min_disk_free_bytes / 2**30:.2f} GiB"
                        )
                        self.fatal_event.set()
                    if self.table and started >= next_tablet:
                        tablets = self.snapshot_tablets()
                        tablet_writer.writerow(
                            {"epoch": epoch, "utc_time": utc_now(), "stage": self.stage, **tablets}
                        )
                        tablet_file.flush()
                        with self.lock:
                            self.latest_tablets = tablets
                        next_tablet = started + self.tablet_interval
                except Exception as exc:  # keep one transient monitoring failure from aborting the load
                    print(f"[{utc_now()}] monitor warning ({self.stage}): {exc}", flush=True)
                elapsed = time.monotonic() - started
                self.stop_event.wait(max(0.05, self.interval - elapsed))

    def current(self) -> Tuple[Dict[str, float], Dict[str, float]]:
        with self.lock:
            return dict(self.latest_metrics), dict(self.latest_tablets)


class HttpLogGenerator:
    def __init__(self, seed: int, response_body_bytes: int):
        self.rng = random.Random(seed)
        self.response_body_bytes = response_body_bytes
        self.row_id = 0
        self.methods = ["GET", "GET", "GET", "POST", "PUT"]
        self.paths = ["/index.html", "/api/login", "/api/search", "/assets/app.js", "/download/report"]
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 Chrome/92.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15",
            "curl/8.7.1",
            "okhttp/4.12.0",
        ]

    def _ip(self, private: bool = False) -> str:
        if private:
            return f"10.{self.rng.randrange(1, 224)}.{self.rng.randrange(0, 256)}.{self.rng.randrange(1, 255)}"
        return f"{self.rng.randrange(20, 224)}.{self.rng.randrange(0, 256)}.{self.rng.randrange(0, 256)}.{self.rng.randrange(1, 255)}"

    def _mac(self) -> str:
        return ":".join(f"{self.rng.randrange(0, 256):02x}" for _ in range(6))

    def make_row(self, logical_second: int) -> Dict[str, object]:
        self.row_id += 1
        event_time = BASE_EVENT_TIME + dt.timedelta(seconds=logical_second)
        event_ts = int(event_time.timestamp())
        method = self.rng.choice(self.methods)
        path = self.rng.choice(self.paths)
        dst_ip = self._ip()
        src_ip = self._ip(private=True)
        status = self.rng.choices([200, 201, 302, 400, 403, 404, 500], [70, 3, 5, 4, 4, 10, 4])[0]
        random_bytes = self.rng.randbytes(max(1, self.response_body_bytes * 3 // 4))
        response_body = base64.b64encode(random_bytes).decode("ascii")[: self.response_body_bytes]
        suffix = f"{self.rng.randrange(36**6):06x}"[-6:]
        uuid = f"http_log-{event_ts}-{self.row_id}-{suffix}"
        user_agent = self.rng.choice(self.user_agents)
        request_header = (
            f"{method} {path} HTTP/1.1\r\nHost: {dst_ip}\r\nConnection: close\r\n"
            f"User-Agent: {user_agent}\r\nAccept-Encoding: gzip, deflate, br\r\n"
            "Accept: text/html,application/json,*/*;q=0.8\r\n"
        )
        response_header = (
            f"HTTP/1.1 {status} {'OK' if status < 400 else 'ERROR'}\r\nServer: Microsoft-IIS/8.5\r\n"
            f"Content-Type: {'text/html' if path.endswith('.html') else 'application/json'}\r\n"
            f"Connection: close\r\nContent-Length: {len(response_body)}\r\n"
        )
        trace_info = "|".join(
            json.dumps({"appName": app, "timestamp": event_ts + offset}, separators=(",", ":"))
            for offset, app in enumerate(("logDetect", "logProduct", "logLanding", "logUpload", "transfer"))
        )
        src_ip_int = int(ipaddress.IPv4Address(src_ip))
        dst_ip_int = int(ipaddress.IPv4Address(dst_ip))
        upload_ts = event_ts + self.rng.randrange(1, 10)
        insert_ts = upload_ts + self.rng.randrange(0, 5)
        request_body = json.dumps(
            {"q": suffix, "page": self.rng.randrange(1, 100), "account": f"user_{self.row_id % 10000}"},
            separators=(",", ":"),
        )
        body_is_indexed = self.rng.random() < 0.9
        extensions = json.dumps(
            {
                "appName": "collect",
                "timestamp": event_ts,
                "sample": self.rng.randrange(1000000),
                "userDefine": {
                    "userDefineInt1": self.rng.randrange(0, 1000),
                    "userDefineInt2": self.rng.randrange(0, 1000),
                    "userDefineInt3": self.rng.randrange(0, 1000),
                    "userDefineInt4": self.rng.randrange(0, 1000),
                    "userDefineLong1": self.rng.randrange(0, 10**12),
                    "userDefineLong2": self.rng.randrange(0, 10**12),
                    "userDefineString1": f"rule-{self.rng.randrange(100)}",
                    "userDefineString2": f"policy-{self.rng.randrange(100)}",
                    "userDefineString3": "http",
                    "userDefineString4": method,
                    "userDefineString5": path,
                    "userDefineString6": suffix,
                    "userDefineBool1": status < 400,
                    "userDefineBool2": body_is_indexed,
                },
            },
            separators=(",", ":"),
        )
        return {
            "respStatus": status,
            "recordTimestamp": event_ts,
            "reqMethod": method,
            "tenant": str(10001001 + self.rng.randrange(0, 8)),
            "uuId": uuid,
            "v": 1,
            "customer": str(10001001 + self.rng.randrange(0, 8)),
            "uploadTimestamp": upload_ts,
            "insertTimestamp": insert_ts,
            "recordTime": event_time.strftime("%Y-%m-%d %H:%M:%S"),
            "uploadTime": (event_time + dt.timedelta(seconds=upload_ts - event_ts)).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "vendor": "sangfor",
            "productType": "STA",
            "productVer": f"STA3.0.{self.rng.randrange(80, 120)}",
            "manage": f"F{self.rng.randrange(16**7):07X}",
            "originProductType": "STA",
            "originProductVer": "STA3.0.99",
            "deviceId": f"F{self.rng.randrange(16**7):07X}",
            "sessionId": f"session-{event_ts}-{self.rng.randrange(16**8):08x}",
            "httpId": f"http-{src_ip}-{dst_ip}-{event_ts}-{suffix}",
            "srcIp": src_ip,
            "srcPort": self.rng.randrange(1024, 65536),
            "srcMac": self._mac(),
            "srcIpTag": 0,
            "srcType": self.rng.randrange(1, 10),
            "srcSubType": self.rng.randrange(1, 100),
            "srcCountry": "LAN",
            "srcProvince": "LAN",
            "srcCity": "LAN",
            "dstIp": dst_ip,
            "dstPort": self.rng.choice([80, 443, 8080, 8443]),
            "dstMac": self._mac(),
            "dstIpTag": 1,
            "dstType": self.rng.randrange(1, 10),
            "dstSubType": self.rng.randrange(1, 100),
            "dstCountry": self.rng.choice(["China", "Brazil", "United States", "Germany", "Singapore"]),
            "dstProvince": self.rng.choice(["Shanghai", "Sao Paulo", "California", "Hesse", "Singapore"]),
            "dstCity": self.rng.choice(["Shanghai", "Sao Paulo", "Los Angeles", "Frankfurt", "Singapore"]),
            "uri": path,
            "url": f"http://{dst_ip}{path}",
            "userAgent": user_agent,
            "xForwardedFor": f"{src_ip}, 10.223.5.{self.rng.randrange(1, 255)}",
            "reqContentType": "application/json" if method != "GET" else "text/html",
            "respContentType": "text/html" if path.endswith(".html") else "application/json",
            "respServer": "Microsoft-IIS/8.5",
            "duration": self.rng.randrange(1, 5000),
            "requestHead": request_header,
            "responseHead": response_header,
            "requestBody": request_body,
            "responseBody": response_body if body_is_indexed else "",
            "hostIp": dst_ip,
            "hostIpValid": "true",
            "moduleType": "http",
            "logType": "http_log",
            "manageIp": "10.223.5.216",
            "deviceIp": "10.223.5.216",
            "logSampled": self.rng.randrange(0, 2),
            "logTraceInfo": trace_info,
            "urlHash": zlib.crc32(f"http://{dst_ip}{path}".encode()),
            "encryptType": self.rng.randrange(0, 4),
            "encPayloadType": self.rng.randrange(-1, 4),
            "encReqType": self.rng.randrange(-1, 4),
            "encRspType": self.rng.randrange(-1, 4),
            "srcAssetId": f"src-asset-{src_ip_int:08x}",
            "dstAssetId": f"dst-asset-{dst_ip_int:08x}",
            "srcIp_v4_int": src_ip_int,
            "srcIp_v6_int": 0,
            "dstIp_v4_int": dst_ip_int,
            "dstIp_v6_int": 0,
            "hostIp_v4_int": [dst_ip_int],
            "hostIp_v6_int": [0],
            "cloudTs": insert_ts,
            "srcRegionId": "LAN",
            "dstRegionId": f"region-{self.rng.randrange(1, 100)}",
            "extensions": extensions,
            "xForwardedForTag": 0,
            "reqBodyLen": len(request_body),
            "respBodyLen": len(response_body),
            "rawMsg": f"{request_header}\r\n{request_body}\r\n{response_header}",
            "greSrcIp": [src_ip],
            "greDstIp": [dst_ip],
            "vxlanId": [self.rng.randrange(1, 16000000)],
            "vlanId": [self.rng.randrange(1, 4095)],
            "mplsLabel": [self.rng.randrange(16, 1048576)],
            "tunnelProtocol": [self.rng.choice(["GRE", "VXLAN", "MPLS"])],
            "devUId": self.rng.randrange(1, 1000000),
            "host": dst_ip,
            "reqDataGrade": self.rng.randrange(0, 5),
            "reqDataClassifications": ["account", "query"],
            "reqDataLabelIds": [f"label-{self.rng.randrange(100)}"],
            "reqDataCounts": [1, self.rng.randrange(1, 10)],
            "reqDataValues": json.dumps({"account": f"user_{self.row_id % 10000}", "q": suffix}),
            "respDataGrade": self.rng.randrange(0, 5),
            "respDataClassifications": ["web", "content"],
            "respDataLabelIds": [f"label-{self.rng.randrange(100)}"],
            "respDataCounts": [1, len(response_body)],
            "respDataValues": json.dumps({"status": status, "length": len(response_body)}),
            "apiId": f"api-{zlib.crc32(path.encode()):08x}",
            "apiName": path,
            "apiTags": [self.rng.randrange(1, 100), self.rng.randrange(100, 200)],
            "accountId": f"account-{self.row_id % 10000}",
            "accountName": f"user_{self.row_id % 10000}",
            "accountProofId": f"proof-{suffix}",
            "xAppId": "logProduct",
            "xAppName": "HTTP Log Product",
            "xAppGroup": "log",
            "xAppTags": ["http", "audit"],
            "xUserId": f"user-{self.row_id % 10000}",
            "xUserName": f"user_{self.row_id % 10000}",
            "xUserDomain": "example.local",
            "xUserGroup": "employees",
            "xUserGroupId": "group-1",
            "xUserSource": "http",
            "xUserRoles": ["reader", "web-user"],
            "xUserTags": ["normal", "office"],
            "xUserProofId": f"user-proof-{suffix}",
            "xffClientIp": src_ip,
            "xffClientIpTag": 0,
            "xffClientIpType": self.rng.randrange(1, 10),
            "xffClientIpSubType": self.rng.randrange(1, 100),
            "xffClientIpAssetId": f"src-asset-{src_ip_int:08x}",
            "xffClientIpRegionId": "LAN",
            "responseBodyNoIndex": "" if body_is_indexed else response_body,
            "isResponseBodyIndex": body_is_indexed,
        }

    def make_batch(self, logical_second: int, target_bytes: int) -> Tuple[bytes, int]:
        lines: List[bytes] = []
        size = 0
        while size < target_bytes:
            encoded = json.dumps(
                self.make_row(logical_second), ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8") + b"\n"
            lines.append(encoded)
            size += len(encoded)
        return b"".join(lines), len(lines)


class StreamLoader:
    def __init__(
        self,
        fe_http_url: str,
        be_http_url: str,
        database: str,
        user: str,
        password: str,
        timeout: int,
    ):
        self.fe_http_url = fe_http_url.rstrip("/")
        self.be_http_url = be_http_url.rstrip("/")
        self.database = database
        self.auth = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()
        self.timeout = timeout

    def load(self, table: str, label: str, payload: bytes) -> Dict[str, object]:
        url = f"{self.fe_http_url}/api/{self.database}/{table}/_stream_load"
        headers = {
            "Authorization": self.auth,
            "label": label,
            "format": "json",
            "read_json_by_line": "true",
            "strict_mode": "true",
            "max_filter_ratio": "0",
            "timeout": str(self.timeout),
            "timezone": "Asia/Shanghai",
            "Content-Type": "application/x-ndjson",
            "Expect": "100-continue",
        }
        raw = ""
        for _hop in range(3):
            request = urllib.request.Request(url, data=payload, method="PUT", headers=headers)
            try:
                with HTTP_OPENER.open(request, timeout=self.timeout + 10) as response:
                    raw = response.read().decode("utf-8", errors="replace")
                break
            except urllib.error.HTTPError as exc:
                if exc.code not in (307, 308) or not exc.headers.get("Location"):
                    raise
                redirected = urllib.parse.urlsplit(exc.headers["Location"])
                external_be = urllib.parse.urlsplit(self.be_http_url)
                url = urllib.parse.urlunsplit(
                    (external_be.scheme, external_be.netloc, redirected.path, redirected.query, redirected.fragment)
                )
        else:
            raise RuntimeError("too many Stream Load redirects")
        try:
            result = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"non-JSON Stream Load response: {raw[:1000]}") from exc
        if result.get("Status") not in ("Success", "Publish Timeout"):
            raise RuntimeError(f"Stream Load failed: {json.dumps(result, ensure_ascii=False)}")
        if int(result.get("NumberFilteredRows", 0)) != 0:
            raise RuntimeError(f"Stream Load filtered rows: {json.dumps(result, ensure_ascii=False)}")
        return result


def read_configs(mysql: MysqlClient) -> Dict[str, str]:
    names = (
        "'base_compaction_interval_seconds_since_last_operation',"
        "'base_compaction_check_interval_seconds'"
    )
    rows = mysql.query_rows(
        f"SELECT NAME,VALUE FROM information_schema.be_configs WHERE NAME IN ({names}) ORDER BY NAME"
    )
    return {row["NAME"]: row["VALUE"] for row in rows}


def set_configs(mysql: MysqlClient, interval: int, check: int) -> None:
    mysql.execute(
        "UPDATE information_schema.be_configs SET VALUE = "
        f"'{interval}' WHERE NAME = 'base_compaction_interval_seconds_since_last_operation';"
        "UPDATE information_schema.be_configs SET VALUE = "
        f"'{check}' WHERE NAME = 'base_compaction_check_interval_seconds';"
    )
    actual = read_configs(mysql)
    if actual.get("base_compaction_interval_seconds_since_last_operation") != str(interval):
        raise RuntimeError(f"failed to set base compaction interval: {actual}")
    if actual.get("base_compaction_check_interval_seconds") != str(check):
        raise RuntimeError(f"failed to set base compaction check interval: {actual}")


def setup_tables(mysql: MysqlClient, recreate: bool) -> None:
    mysql.execute("CREATE DATABASE IF NOT EXISTS zc_test")
    existing = mysql.query_rows(
        "SELECT TABLE_NAME FROM information_schema.TABLES "
        f"WHERE TABLE_SCHEMA='zc_test' AND TABLE_NAME IN ('{TABLE_FINE}','{TABLE_COARSE}')"
    )
    if existing and not recreate:
        names = ", ".join(row["TABLE_NAME"] for row in existing)
        raise RuntimeError(f"test tables already exist ({names}); pass --recreate-tables to replace them")
    if recreate:
        mysql.execute(
            f"DROP TABLE IF EXISTS zc_test.{TABLE_FINE};"
            f"DROP TABLE IF EXISTS zc_test.{TABLE_COARSE};"
        )
    mysql.execute_script(SCHEMA_PATH)


def check_backends(mysql: MysqlClient) -> None:
    rows = mysql.query_rows("SHOW BACKENDS")
    if not rows or any(row.get("Alive", "").lower() != "true" for row in rows):
        raise RuntimeError(f"not all backends are alive: {rows}")


def active_compaction_count(mysql: MysqlClient) -> int:
    return len(mysql.query_rows("SHOW PROC '/compactions'"))


def wait_until_no_existing_compaction(mysql: MysqlClient, metrics_url: str, timeout: int = 600) -> None:
    deadline = time.monotonic() + timeout
    stable_since: Optional[float] = None
    while time.monotonic() < deadline:
        try:
            with HTTP_OPENER.open(metrics_url, timeout=10) as response:
                metrics = parse_prometheus(response.read().decode(errors="replace"))
        except Exception as exc:
            print(f"[{utc_now()}] metrics not ready while waiting for idle: {exc}", flush=True)
            stable_since = None
            time.sleep(2)
            continue
        idle = (
            metrics["running_base"] == 0
            and metrics["running_cumulative"] == 0
            and metrics["wait_base"] == 0
            and metrics["wait_cumulative"] == 0
            and active_compaction_count(mysql) == 0
        )
        if idle:
            stable_since = stable_since or time.monotonic()
            if time.monotonic() - stable_since >= 10:
                return
        else:
            stable_since = None
        time.sleep(2)
    raise RuntimeError("existing compaction tasks did not become idle within timeout")


def write_restore_sql(path: Path, configs: Dict[str, str]) -> None:
    path.write_text(
        "UPDATE information_schema.be_configs SET VALUE = "
        f"'{configs['base_compaction_interval_seconds_since_last_operation']}' "
        "WHERE NAME = 'base_compaction_interval_seconds_since_last_operation';\n"
        "UPDATE information_schema.be_configs SET VALUE = "
        f"'{configs['base_compaction_check_interval_seconds']}' "
        "WHERE NAME = 'base_compaction_check_interval_seconds';\n",
        encoding="utf-8",
    )


def run_load(
    args: argparse.Namespace,
    run_id: str,
    table: str,
    output_dir: Path,
    sampler: MetricsSampler,
    loader: StreamLoader,
) -> Dict[str, object]:
    generator = HttpLogGenerator(args.seed, args.response_body_bytes)
    target_bytes = int(args.target_mib_per_second * 2**20 / args.loads_per_second)
    total_batches = args.duration_seconds * args.loads_per_second
    output_dir.mkdir(parents=True, exist_ok=True)
    load_path = output_dir / "loads.csv"
    fields = [
        "batch",
        "logical_second",
        "utc_time",
        "label",
        "payload_bytes",
        "rows",
        "latency_ms",
        "status",
        "loaded_rows",
        "loaded_bytes",
        "load_time_ms",
    ]
    total_payload = 0
    total_rows = 0
    load_start = time.monotonic()
    with load_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for batch in range(total_batches):
            if sampler.fatal_event.is_set():
                raise RuntimeError(f"safety stop: {sampler.fatal_reason}")
            check_backends(mysql=args.mysql_client)
            logical_second = batch // args.loads_per_second
            scheduled = load_start + batch / args.loads_per_second
            while time.monotonic() < scheduled:
                if sampler.fatal_event.wait(min(0.2, scheduled - time.monotonic())):
                    raise RuntimeError(f"safety stop: {sampler.fatal_reason}")
            payload, rows = generator.make_batch(logical_second, target_bytes)
            label = f"zc_cmp_{run_id}_{table[-6:]}_{batch:05d}"
            started = time.monotonic()
            result = loader.load(table, label, payload)
            latency_ms = (time.monotonic() - started) * 1000
            loaded_rows = int(result.get("NumberLoadedRows", rows))
            if loaded_rows != rows:
                raise RuntimeError(f"row mismatch for {label}: generated={rows}, loaded={loaded_rows}")
            writer.writerow(
                {
                    "batch": batch,
                    "logical_second": logical_second,
                    "utc_time": utc_now(),
                    "label": label,
                    "payload_bytes": len(payload),
                    "rows": rows,
                    "latency_ms": round(latency_ms, 3),
                    "status": result.get("Status", ""),
                    "loaded_rows": loaded_rows,
                    "loaded_bytes": int(result.get("LoadBytes", 0)),
                    "load_time_ms": int(result.get("LoadTimeMs", 0)),
                }
            )
            handle.flush()
            total_payload += len(payload)
            total_rows += rows
            if batch % max(1, 30 * args.loads_per_second) == 0 or batch + 1 == total_batches:
                metrics, tablets = sampler.current()
                print(
                    f"[{utc_now()}] {table}: {batch + 1}/{total_batches} loads, "
                    f"{total_rows} rows, {total_payload / 2**20:.1f} MiB raw, "
                    f"load={latency_ms:.0f} ms, base/cumu running="
                    f"{metrics.get('running_base', 0):.0f}/{metrics.get('running_cumulative', 0):.0f}, "
                    f"rowsets={tablets.get('total_rowsets', 0):.0f}, "
                    f"disk_free={metrics.get('disk_avail_bytes', 0) / 2**30:.2f} GiB",
                    flush=True,
                )
        final_schedule = load_start + args.duration_seconds
        while time.monotonic() < final_schedule:
            if sampler.fatal_event.wait(min(0.2, final_schedule - time.monotonic())):
                raise RuntimeError(f"safety stop: {sampler.fatal_reason}")
    return {
        "table": table,
        "batches": total_batches,
        "rows": total_rows,
        "payload_bytes": total_payload,
        "load_duration_seconds": time.monotonic() - load_start,
    }


def wait_for_drain(
    args: argparse.Namespace,
    table: str,
    sampler: MetricsSampler,
    load_finished_at: float,
) -> float:
    earliest = load_finished_at + args.base_interval + args.base_check + args.drain_extra_seconds
    deadline = load_finished_at + args.drain_timeout_seconds
    stable: deque = deque(maxlen=max(2, math.ceil(args.drain_stable_seconds / 10)))
    last_print = 0.0
    while time.monotonic() < deadline:
        if sampler.fatal_event.is_set():
            raise RuntimeError(f"safety stop while draining: {sampler.fatal_reason}")
        check_backends(args.mysql_client)
        tablets = sampler.snapshot_tablets()
        metrics, _ = sampler.current()
        active = active_compaction_count(args.mysql_client)
        state = (
            tablets.get("total_rowsets", -1),
            tablets.get("total_segments", -1),
            tablets.get("data_size", -1),
        )
        stable.append(state)
        globally_idle = (
            metrics.get("running_base", 0) == 0
            and metrics.get("running_cumulative", 0) == 0
            and metrics.get("wait_base", 0) == 0
            and metrics.get("wait_cumulative", 0) == 0
            and active == 0
        )
        table_stable = len(stable) == stable.maxlen and len(set(stable)) == 1
        now = time.monotonic()
        if now >= earliest and globally_idle and table_stable:
            drained = now - load_finished_at
            print(
                f"[{utc_now()}] {table}: compaction drained after {drained:.1f}s; "
                f"rowsets={tablets.get('total_rowsets', 0):.0f}, "
                f"segments={tablets.get('total_segments', 0):.0f}",
                flush=True,
            )
            return drained
        if now - last_print >= 30:
            wait_left = max(0.0, earliest - now)
            print(
                f"[{utc_now()}] {table}: draining, minimum_wait_left={wait_left:.0f}s, "
                f"active={active}, base/cumu={metrics.get('running_base', 0):.0f}/"
                f"{metrics.get('running_cumulative', 0):.0f}, "
                f"rowsets={tablets.get('total_rowsets', 0):.0f}",
                flush=True,
            )
            last_print = now
        time.sleep(10)
    raise RuntimeError(f"{table} compaction did not drain within {args.drain_timeout_seconds}s")


def read_csv_numbers(path: Path) -> List[Dict[str, float]]:
    if not path.exists():
        return []
    rows: List[Dict[str, float]] = []
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            converted: Dict[str, float] = {}
            for key, value in row.items():
                try:
                    converted[key] = float(value or 0)
                except (TypeError, ValueError):
                    continue
            rows.append(converted)
    return rows


def summarize_stage(stage_dir: Path, load_result: Optional[Dict[str, object]]) -> Dict[str, float]:
    metrics = read_csv_numbers(stage_dir / "metrics.csv")
    tablets = read_csv_numbers(stage_dir / "tablets.csv")
    loads = read_csv_numbers(stage_dir / "loads.csv")
    summary: Dict[str, float] = {}
    if metrics:
        first, last = metrics[0], metrics[-1]
        duration = max(0.001, last.get("epoch", 0) - first.get("epoch", 0))
        summary["observed_seconds"] = duration
        for counter in (
            "base_compaction_bytes",
            "cumulative_compaction_bytes",
            "base_compaction_deltas",
            "cumulative_compaction_deltas",
            "disk_read_bytes",
            "disk_write_bytes",
            "disk_io_time_ms",
        ):
            summary[f"delta_{counter}"] = max(0.0, last.get(counter, 0) - first.get(counter, 0))
        cpu_fields = [
            "cpu_idle",
            "cpu_iowait",
            "cpu_user",
            "cpu_system",
            "cpu_nice",
            "cpu_irq",
            "cpu_soft_irq",
            "cpu_steal",
        ]
        cpu_deltas = {field: max(0.0, last.get(field, 0) - first.get(field, 0)) for field in cpu_fields}
        cpu_total = sum(cpu_deltas.values())
        summary["cpu_busy_percent"] = (
            100.0 * (cpu_total - cpu_deltas["cpu_idle"] - cpu_deltas["cpu_iowait"]) / cpu_total
            if cpu_total
            else 0.0
        )
        summary["cpu_iowait_percent"] = 100.0 * cpu_deltas["cpu_iowait"] / cpu_total if cpu_total else 0.0
        summary["disk_busy_percent"] = min(100.0, summary["delta_disk_io_time_ms"] / (duration * 10.0))
        for field in (
            "running_base",
            "running_cumulative",
            "wait_base",
            "wait_cumulative",
            "compaction_mem_bytes",
            "process_mem_bytes",
            "tablet_base_max_score",
            "tablet_cumulative_max_score",
            "base_compaction_cost_ms",
            "cumulative_compaction_cost_ms",
        ):
            values = [row.get(field, 0) for row in metrics]
            summary[f"max_{field}"] = max(values, default=0.0)
            summary[f"p95_{field}"] = percentile(values, 0.95)
        summary["min_disk_avail_bytes"] = min((row.get("disk_avail_bytes", 0) for row in metrics), default=0)
        summary["base_active_sample_percent"] = 100 * statistics.mean(
            1.0 if row.get("running_base", 0) > 0 else 0.0 for row in metrics
        )
        summary["cumulative_active_sample_percent"] = 100 * statistics.mean(
            1.0 if row.get("running_cumulative", 0) > 0 else 0.0 for row in metrics
        )
    if tablets:
        for field in (
            "total_versions",
            "max_versions",
            "total_rowsets",
            "max_rowsets",
            "total_segments",
            "max_segments",
            "data_size",
            "num_rows",
        ):
            values = [row.get(field, 0) for row in tablets]
            summary[f"peak_{field}"] = max(values, default=0.0)
            summary[f"final_{field}"] = values[-1] if values else 0.0
    if loads:
        summary["load_count"] = float(len(loads))
        summary["raw_payload_bytes"] = sum(row.get("payload_bytes", 0) for row in loads)
        summary["loaded_rows"] = sum(row.get("loaded_rows", 0) for row in loads)
        latencies = [row.get("latency_ms", 0) for row in loads]
        summary["load_latency_avg_ms"] = statistics.mean(latencies) if latencies else 0.0
        summary["load_latency_p95_ms"] = percentile(latencies, 0.95)
        summary["load_latency_max_ms"] = max(latencies, default=0.0)
    if load_result:
        summary["drain_seconds"] = float(load_result.get("drain_seconds", 0))
    raw = summary.get("raw_payload_bytes", 0)
    compaction = summary.get("delta_base_compaction_bytes", 0) + summary.get(
        "delta_cumulative_compaction_bytes", 0
    )
    summary["compaction_bytes_per_raw_byte"] = compaction / raw if raw else 0.0
    return summary


def write_summary(run_dir: Path, run_results: Dict[str, Dict[str, object]]) -> Dict[str, object]:
    baseline = summarize_stage(run_dir / "baseline", None)
    fine = summarize_stage(run_dir / TABLE_FINE, run_results.get(TABLE_FINE))
    coarse = summarize_stage(run_dir / TABLE_COARSE, run_results.get(TABLE_COARSE))
    summary: Dict[str, object] = {"baseline": baseline, TABLE_FINE: fine, TABLE_COARSE: coarse}
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    def mib(value: float) -> float:
        return value / 2**20

    lines = [
        "# StarRocks compaction partition benchmark",
        "",
        "| Metric | 7 x 5-minute partitions | 1 x 35-minute partition |",
        "|---|---:|---:|",
    ]
    rows = [
        ("Loaded rows", "loaded_rows", lambda x: f"{x:.0f}"),
        ("Raw input MiB", "raw_payload_bytes", lambda x: f"{mib(x):.1f}"),
        ("Table data MiB (final)", "final_data_size", lambda x: f"{mib(x):.1f}"),
        ("Load latency avg ms", "load_latency_avg_ms", lambda x: f"{x:.1f}"),
        ("Load latency p95 ms", "load_latency_p95_ms", lambda x: f"{x:.1f}"),
        ("Base compaction MiB", "delta_base_compaction_bytes", lambda x: f"{mib(x):.1f}"),
        ("Cumulative compaction MiB", "delta_cumulative_compaction_bytes", lambda x: f"{mib(x):.1f}"),
        ("Compaction bytes / raw byte", "compaction_bytes_per_raw_byte", lambda x: f"{x:.3f}"),
        ("Base active samples %", "base_active_sample_percent", lambda x: f"{x:.2f}"),
        ("Cumulative active samples %", "cumulative_active_sample_percent", lambda x: f"{x:.2f}"),
        ("Peak total rowsets", "peak_total_rowsets", lambda x: f"{x:.0f}"),
        ("Peak max rowsets/tablet", "peak_max_rowsets", lambda x: f"{x:.0f}"),
        ("Peak cumulative score", "max_tablet_cumulative_max_score", lambda x: f"{x:.0f}"),
        ("CPU busy %", "cpu_busy_percent", lambda x: f"{x:.2f}"),
        ("CPU iowait %", "cpu_iowait_percent", lambda x: f"{x:.2f}"),
        ("Disk busy %", "disk_busy_percent", lambda x: f"{x:.2f}"),
        ("Minimum disk free GiB", "min_disk_avail_bytes", lambda x: f"{x / 2**30:.2f}"),
        ("Drain seconds", "drain_seconds", lambda x: f"{x:.1f}"),
    ]
    for label, key, formatter in rows:
        lines.append(f"| {label} | {formatter(fine.get(key, 0))} | {formatter(coarse.get(key, 0))} |")
    lines.extend(
        [
            "",
            "Global compaction counters can contain unrelated internal-table compactions. The raw CSV files are retained",
            "for checking time windows and subtracting the measured idle baseline when needed.",
            "",
        ]
    )
    (run_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mysql-host", default="127.0.0.1")
    parser.add_argument("--mysql-port", type=int, default=9130)
    parser.add_argument("--fe-http-url", default="http://127.0.0.1:8130")
    parser.add_argument("--be-http-url", default="http://127.0.0.1:8140")
    parser.add_argument("--be-metrics-url", default="http://127.0.0.1:8140/metrics")
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default=os.environ.get("STARROCKS_PASSWORD", ""))
    parser.add_argument("--duration-seconds", type=int, default=2100)
    parser.add_argument("--loads-per-second", type=int, default=1)
    parser.add_argument("--target-mib-per-second", type=float, default=0.5)
    parser.add_argument("--response-body-bytes", type=int, default=12288)
    parser.add_argument("--seed", type=int, default=20260828)
    parser.add_argument("--base-interval", type=int, default=300)
    parser.add_argument("--base-check", type=int, default=1)
    parser.add_argument("--baseline-seconds", type=int, default=60)
    parser.add_argument("--drain-extra-seconds", type=int, default=10)
    parser.add_argument("--drain-stable-seconds", type=int, default=60)
    parser.add_argument("--drain-timeout-seconds", type=int, default=1200)
    parser.add_argument("--min-disk-free-gib", type=float, default=20.0)
    parser.add_argument("--stream-load-timeout", type=int, default=120)
    parser.add_argument("--results-dir", type=Path, default=SCRIPT_DIR / "results")
    parser.add_argument("--recreate-tables", action="store_true")
    parser.add_argument("--run-id", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.duration_seconds > 2100:
        raise ValueError("duration-seconds cannot exceed the 35-minute partition range (2100 seconds)")
    if args.loads_per_second < 1 or args.target_mib_per_second <= 0:
        raise ValueError("load rate must be positive")
    run_id = args.run_id or dt.datetime.now().strftime("%Y%m%dT%H%M%S")
    run_dir = args.results_dir.resolve() / run_id
    if run_dir.exists():
        raise RuntimeError(f"run directory already exists: {run_dir}")
    run_dir.mkdir(parents=True)

    mysql = MysqlClient(args.mysql_host, args.mysql_port, args.user, args.password)
    args.mysql_client = mysql
    original_configs: Dict[str, str] = {}
    configured = False
    run_results: Dict[str, Dict[str, object]] = {}

    def interrupt_handler(signum: int, _frame: object) -> None:
        raise KeyboardInterrupt(f"received signal {signum}")

    signal.signal(signal.SIGTERM, interrupt_handler)
    signal.signal(signal.SIGINT, interrupt_handler)

    try:
        check_backends(mysql)
        setup_tables(mysql, args.recreate_tables)
        original_configs = read_configs(mysql)
        write_restore_sql(run_dir / "restore_config.sql", original_configs)
        manifest = {
            "run_id": run_id,
            "created_at": utc_now(),
            "arguments": {key: str(value) for key, value in vars(args).items() if key != "mysql_client" and key != "password"},
            "original_configs": original_configs,
            "test_configs": {
                "base_compaction_interval_seconds_since_last_operation": args.base_interval,
                "base_compaction_check_interval_seconds": args.base_check,
            },
        }
        (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        print(f"[{utc_now()}] run directory: {run_dir}", flush=True)
        print(f"[{utc_now()}] waiting for pre-existing compactions to become idle", flush=True)
        wait_until_no_existing_compaction(mysql, args.be_metrics_url)

        set_configs(mysql, args.base_interval, args.base_check)
        configured = True
        print(
            f"[{utc_now()}] compaction time scale applied: interval={args.base_interval}s, check={args.base_check}s",
            flush=True,
        )

        baseline_sampler = MetricsSampler(
            mysql,
            args.be_metrics_url,
            run_dir / "baseline",
            "baseline",
            None,
            int(args.min_disk_free_gib * 2**30),
        )
        baseline_sampler.start()
        print(f"[{utc_now()}] collecting {args.baseline_seconds}s idle baseline", flush=True)
        if baseline_sampler.fatal_event.wait(args.baseline_seconds):
            raise RuntimeError(f"safety stop: {baseline_sampler.fatal_reason}")
        baseline_sampler.stop()

        loader = StreamLoader(
            args.fe_http_url,
            args.be_http_url,
            "zc_test",
            args.user,
            args.password,
            args.stream_load_timeout,
        )
        for table in (TABLE_FINE, TABLE_COARSE):
            print(f"[{utc_now()}] starting sequential stage: {table}", flush=True)
            sampler = MetricsSampler(
                mysql,
                args.be_metrics_url,
                run_dir / table,
                table,
                table,
                int(args.min_disk_free_gib * 2**30),
            )
            sampler.start()
            try:
                result = run_load(args, run_id, table, run_dir / table, sampler, loader)
                load_finished_at = time.monotonic()
                result["drain_seconds"] = wait_for_drain(args, table, sampler, load_finished_at)
                run_results[table] = result
                (run_dir / table / "result.json").write_text(
                    json.dumps(result, indent=2), encoding="utf-8"
                )
            finally:
                sampler.stop()
            print(f"[{utc_now()}] stage fully drained: {table}", flush=True)
        summary = write_summary(run_dir, run_results)
        print(json.dumps(summary, indent=2), flush=True)
        print(f"[{utc_now()}] benchmark complete: {run_dir / 'summary.md'}", flush=True)
        return 0
    finally:
        if configured and original_configs:
            try:
                set_configs(
                    mysql,
                    int(original_configs["base_compaction_interval_seconds_since_last_operation"]),
                    int(original_configs["base_compaction_check_interval_seconds"]),
                )
                print(f"[{utc_now()}] restored original compaction configs: {original_configs}", flush=True)
            except Exception as exc:
                print(
                    f"[{utc_now()}] ERROR restoring configs: {exc}; run {run_dir / 'restore_config.sql'} manually",
                    file=sys.stderr,
                    flush=True,
                )


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt as exc:
        print(f"[{utc_now()}] interrupted: {exc}", file=sys.stderr, flush=True)
        raise SystemExit(130)
    except Exception as exc:
        print(f"[{utc_now()}] ERROR: {exc}", file=sys.stderr, flush=True)
        raise SystemExit(1)
