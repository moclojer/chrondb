# ChronDB Agent Guide

## Project Overview

ChronDB is a chronological key/value database implemented in Clojure and backed by a Git-like storage engine. Every write produces an immutable commit, enabling full history, time-travel queries, and branching semantics across multiple protocols (HTTP REST, Redis, PostgreSQL).

## Key Features

- Native Clojure API for JVM applications
- REST API for HTTP interfaces and automation
- Redis protocol compatibility for drop-in clients
- PostgreSQL wire protocol for SQL access
- Git-inspired versioning system with immutable history
- Document-oriented storage with temporal queries

## Code Style and Standards

### Clojure Conventions

- Use idiomatic Clojure naming (`kebab-case` for vars, namespaces as `chrondb.*`)
- Prefer pure functions and data transformations
- Add docstrings for public vars and functions
- Follow indentation as enforced by `cljfmt`
- Use descriptive logging that reflects chronological operations

### Project Structure

- Application sources: `src/`
- Developer tooling: `dev/`
- Tests: `test/`
- Native image build helpers: `dev/chrondb/native_image.clj`
- Docker assets: `Dockerfile`
- Configuration examples: `config.example.edn`

## Development Guidelines

### Database Operations

- Preserve chronological ordering and causal relationships
- Handle repository creation and concurrency with JGit primitives
- Surface meaningful errors when Git operations fail (lock contention, missing refs)
- Avoid destructive operations; prefer new commits over in-place mutation

### API Development

- Keep REST, Redis, and SQL protocols behaviorally consistent
- Validate inputs early and return protocol-appropriate errors
- Document new endpoints in `docs/`
- Maintain backwards-compatible defaults unless a migration path exists

### Testing

- Cover time-travel and branching scenarios
- Include integration tests that exercise Git-backed storage
- Add regression tests for concurrency and conflict resolution
- Prefer property-based tests when sequence ordering matters

### Git Architecture Integration

- Understand repository layout under `data/`
- Ensure new features respect branch isolation and merge semantics
- Use commit metadata to encode chronological context (timestamps, authors)
- Reference <https://www.moclojer.com/blog/git-as-database-harnessing-hidden-power-internals-chronological-data-storage/> for architectural details

## Specific Instructions

### When Generating Code

1. Respect chronological data guarantees (no rewrites of immutable commits)
2. Fail fast with actionable error messages and structured logs
3. Leverage `transduce`, `reduce`, or pipelines for efficient data flows
4. Guard shared mutable state; prefer atoms with swap! semantics
5. Profile Git-heavy operations when adding loops or recursion

### When Reviewing Code

1. Confirm time-based queries return correct snapshots
2. Check API parity across protocols
3. Demand unit and integration coverage for new behaviors
4. Inspect for Git storage performance regressions
5. Enforce Clojure idioms and docstring completeness

### Dependencies

- Manage dependencies via `deps.edn`
- Verify compatibility with AGPLv3 license requirements
- Avoid adding dependencies that conflict with GraalVM native image constraints
- Document new libraries and rationale in PR descriptions

## Documentation

- Update `README.md` and `docs/` for major user-facing changes
- Record new API endpoints and wire protocol behaviors
- Provide runnable examples where possible (e.g., `docs/examples-*.md`)
- Keep developer-focused instructions in this `AGENT.md`

## CI/CD Workflows

- `publish-docker.yml` publishes the Docker image to GitHub Container Registry on every push to `main`. The workflow authenticates with `${{ secrets.GITHUB_TOKEN }}`, builds using the project `Dockerfile`, and pushes the `latest` tag to `ghcr.io/<owner>/<repo>`.

## Native Image Support

- Generate the uberjar and GraalVM metadata with `clojure -M:build -- --uberjar`
- The `chrondb.native-image` module creates:
  - `target/native-image-args`
  - `target/filter.json`
  - `target/native-config/`
- Build the native binary locally with `native-image @target/native-image-args -o target/chrondb_local`
- Always include `--features=clj_easy.graal_build_time.InitClojureClasses`
- Do not reintroduce `-H:+UnlockExperimentalVMOptions`; critical JGit classes are already marked for build-time init
- Avoid running the native image build in parallel with services on ports `3000`, `6379`, or `5432`
- Use `.github/workflows/build-native-image.yml` as a reference for the full pipeline and smoke tests

### Advanced Native Image Topics

- **Profile-guided optimization (PGO)**: Capture profile data by running the JVM artifact with representative workloads and feed it to `native-image` via `--pgo`. Keep PGO configs alongside `target/native-image-args`.
- **Closed-world assumptions**: Audit reflection, dynamic class loading, and proxies. Add explicit configuration to `target/native-config/` using `clj-easy/graal-build-time` helpers when new reflective code paths are introduced.
- **Heap configuration**: Tune `--initial-heap-size` and `--max-heap-size` in `target/native-image-args` when targeting constrained environments. Document rationale and expected memory footprint.
- **Monitoring hooks**: When adding logging bridges or metrics, ensure required classes are initialized at build time or registered via `native-image` resources to avoid startup penalties.
- **Determinism**: Native builds must remain reproducible. Capture environment-sensitive values (timestamps, random seeds) explicitly during the `:build` task to prevent binary drift between runs.

## Docker Image Documentation

- Builder image: `ghcr.io/graalvm/native-image:ol9-java17-22.3.2`
- Runtime base: `debian:12-slim`
- Build steps:
  - Install Clojure via official installer script
  - Run `clojure -M:build -- --uberjar` to produce `target/chrondb.jar`
  - Execute `native-image @target/native-image-args -H:Name=chrondb`
- Runtime layout:
  - Binary copied to `/usr/local/bin/chrondb`
  - Resources copied to `/app/resources`
  - Writable data directory at `/app/data`
  - Exposed ports: `3000` (HTTP), `6379` (Redis), `5432` (PostgreSQL)
  - Non-root user `chrondb` (UID/GID managed in Dockerfile) with home at `/app`
- Usage:
  - Build: `docker build -t moclojer/native-image-builder .`
  - Run: `docker run --rm -p 3000:3000 moclojer/native-image-builder`
  - Persist data: `docker run --rm -v $(pwd)/data:/app/data moclojer/native-image-builder`
