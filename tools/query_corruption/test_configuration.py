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

from pathlib import Path
import re
import unittest


class DedicatedConfigurationTest(unittest.TestCase):
    directory = Path(__file__).resolve().parent
    repository = directory.parent.parent

    def entries(self, path):
        return dict(tuple(part.strip() for part in line.split("=", 1))
                    for line in path.read_text().splitlines() if line.strip() and not line.lstrip().startswith("#"))

    def test_backend_options_exist_and_ports_do_not_overlap(self):
        declared = set(re.findall(r"CONF_[A-Za-z0-9_]+\((\w+),",
                                  (self.repository / "be/src/common/config.h").read_text()))
        ports = set()
        normalized = []
        for backend in range(3):
            config = self.entries(self.directory / "conf" / ("be%d.conf" % backend))
            for key in config:
                if not key.isupper():
                    self.assertIn(key, declared)
            self.assertEqual("0", config["max_compaction_concurrency"])
            self.assertEqual("true", config["disable_storage_page_cache"])
            self.assertEqual("${STARROCKS_HOME}/storage", config["storage_root_path"])
            for key in ("be_http_port", "be_port", "heartbeat_service_port", "brpc_port"):
                port = int(config.pop(key))
                self.assertNotIn(port, ports)
                self.assertGreaterEqual(port, 19400)
                ports.add(port)
            normalized.append(config)
        self.assertEqual(normalized[0], normalized[1])
        self.assertEqual(normalized[0], normalized[2])

    def test_frontend_options_exist_and_start_strict(self):
        declared = (self.repository / "fe/fe-core/src/main/java/com/starrocks/common/Config.java").read_text()
        config = self.entries(self.directory / "conf/fe.conf")
        for key in config:
            if not key.isupper():
                self.assertRegex(declared, r"\b" + key + r"\s*=")
        self.assertEqual("false", config["enable_query_corruption_tolerance"])
        self.assertEqual("19303", config["query_port"])
        self.assertEqual("${STARROCKS_HOME}/meta", config["meta_dir"])

    def test_product_template_opts_in_but_java_default_remains_false(self):
        key = "enable_query_corruption_tolerance"
        config_path = self.repository / "conf/fe.conf"
        self.assertEqual("true", self.entries(config_path)[key])
        self.assertEqual(1, len(re.findall(r"(?m)^\s*" + key + r"\s*=", config_path.read_text())))
        java = (self.repository / "fe/fe-core/src/main/java/com/starrocks/common/Config.java").read_text()
        self.assertRegex(java, r"public static boolean " + key + r"\s*=\s*false\s*;")


if __name__ == "__main__":
    unittest.main()
