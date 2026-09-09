# Repository Instructions

## Tenant-TTL round commits

For Tenant-TTL implementation rounds 007 and later:

- Keep each implementation round in one independent commit and do not mix unrelated changes.
- Use the commit subject format `<type>(<scope>): [NNN] <summary>`.
- Include a detailed commit body with non-empty `Problem`, `Implementation`, `Compatibility`, and `Tests` sections.
- Record the tests that actually ran. If a relevant test was not run, state that fact and the reason instead of implying coverage.

## Tenant-TTL change isolation

For all Tenant-TTL development and review work:

- Never mix a non-Tenant-TTL fix into a Tenant-TTL implementation-round commit.
- Put each independent non-Tenant-TTL issue in its own commit; do not combine multiple unrelated fixes for convenience.
- Treat a pre-existing issue inherited from the upstream/community baseline as a community fix, keep each such issue in its own commit, and use the subject format `<type>(<scope>): [COMMUNITY-FIX] <summary>`.
- Apply the same detailed commit-body and truthful test-reporting requirements above to non-Tenant-TTL and community-fix commits.

## StarRocks build artifact preservation

Treat completed StarRocks compilation output and intermediate build state as durable, reusable assets:

- Reuse the existing incremental build workspace before creating a new one. The current Tenant-TTL build container is `starrocks-tenant-ttl-4.0-build`, based on `starrocks/dev-env-ubuntu:4.0-latest`, with its isolated source and build tree at `/tenant-ttl-workspace`.
- Keep production and unit-test build directories isolated by build type. Use `be/build_Release` for the Release BE build and distinct `be/ut_build_Debug`, `be/ut_build_Release`, `be/ut_build_ASAN`, and `be/ut_build_UBSAN` directories for unit-test configurations. Never configure different build or sanitizer types into the same directory.
- Preserve CMake caches, generated files, object files, dependency files, linked binaries, `output/`, and useful test logs so later source updates can use incremental compilation. Sync only the changed source files into the isolated workspace; do not recopy or clean the entire workspace for an ordinary update.
- Before reusing a build directory, inspect its `CMakeCache.txt` and compile flags to confirm the build type, sanitizer, compiler, and important feature switches match the requested build. Do not infer the configuration from a directory name alone.
- Stopping the build container is allowed, but do not remove the build container, its writable layer, the build image, a build volume, `/tenant-ttl-workspace`, or any `be/build_*`/`be/ut_build_*` directory without explicit user approval. Do not run broad Docker prune or build-clean commands that may remove these assets.
- The current `/tenant-ttl-workspace` resides in the container writable layer rather than a Docker volume. Preserve the container until the workspace has been deliberately migrated or backed up to a durable named volume or host bind mount; never recreate the container first and assume the build cache will survive.
- If disk pressure or cache incompatibility makes cleanup desirable, first report the size, configuration, last useful state, and rebuild cost of each candidate. Prefer preserving or migrating a reusable cache, and obtain user approval before deletion.
