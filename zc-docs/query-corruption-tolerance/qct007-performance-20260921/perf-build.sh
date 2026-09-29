#!/usr/bin/env bash
set -euo pipefail
cd /query-corruption-workspace/src
logs=/query-corruption-workspace/logs/QCT-007
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
export STARROCKS_VERSION=4.0.11-QCT-007
export STARROCKS_COMMIT_HASH=46ca14950dc76004c5df355ce2c975a77c50041a
export BUILD_TYPE=RELEASE
export MAVEN_OPTS=-Xmx2g
export CCACHE_DIR=/query-corruption-workspace/cache/ccache
export CCACHE_BASEDIR=/query-corruption-workspace/src
export CCACHE_NOHASHDIR=true
date -u
git rev-parse HEAD
git diff --exit-code HEAD -- be/src be/CMakeLists.txt 'fe/*/src/main/**' fe/pom.xml 'fe/*/pom.xml' gensrc build.sh env.sh CMakeLists.txt cmake
python3 build-support/gen_build_version.py --cpp be/src/gen_cpp/build/gen_cpp
cmake --build be/build_Release --target starrocks_be -- -j1 > "$logs/perf-be-version-relink.log" 2>&1
date -u
cd fe
mvn -B -pl fe-core -am -Dmaven.clean.skip=true \
  -Dmaven.repo.local=/query-corruption-workspace/cache/maven \
  -Dfe_ut_parallel=1 -DfailIfNoTests=false \
  -Dtest=QueryCorruptionPolicyTest,QueryCorruptionWarningTest,QueryCorruptionPlanTest,QueryCorruptionJsonTest,QueryCorruptionHttpSenderTest,QueryCorruptionJdbcTest,QueryCorruptionHttpTest,MysqlEofPacketTest,MysqlOkPacketTest \
  -Dqct.mysql.driver.jar=/query-corruption-workspace/cache/maven/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar \
  package > "$logs/perf-fe-tests-package.log" 2>&1
date -u
cd ..
LD_LIBRARY_PATH="$JAVA_HOME/lib/server:/var/local/thirdparty/installed/jemalloc/lib-shared" be/output/lib/starrocks_be --version
"$JAVA_HOME/bin/javap" -constants -classpath fe/fe-core/target/starrocks-fe.jar com.starrocks.common.Version
sha256sum be/output/lib/starrocks_be fe/fe-core/target/starrocks-fe.jar
cd tools/query_corruption
/query-corruption-workspace/tools/python/bin/python -m unittest discover -v > "$logs/perf-tool-tests.log" 2>&1
date -u
