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
