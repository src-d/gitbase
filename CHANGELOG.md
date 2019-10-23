# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Only optimize sorted DISTINCT if the first column on the order by is on the distinct schema. ([#976](https://github.com/src-d/gitbase/issues/976))
- Avoid possible panics in LOC by using a safe cache accessor.
- sql: Add length to VARCHAR and CHAR MySQLTypeName strings in `SHOW CREATE TABLE` output.

### Added

- Added BLAME function.
- Better error messages for index failures.
- Implemented subquery expressions.
- Added support for 24-bit integers (MySQL's MEDIUMINT)

### Changed

- Use the minimum integer size as necessary when parsing literals.

## [0.24.0-rc2] - 2019-10-02

### Fixed

- plan: return types in lowercase in SHOW CREATE TABLE ([#827](https://github.com/src-d/go-mysql-server/pull/827))
- analyzer: do not erase sort node when pushing it down ([#818](https://github.com/src-d/go-mysql-server/pull/818))
- Fixed null errors during value comparisons ([#831](https://github.com/src-d/go-mysql-server/pull/831))
- plan: fix race conditions in Exchange node
- Add CHAR and DATETIME types support ([#823](https://github.com/src-d/go-mysql-server/pull/823))
- Also check sockets bind to tcp6 and fail on all closed sockets ([#824](https://github.com/src-d/go-mysql-server/pull/824))

### Changed

- Added LIKE test with newlines ([#820](https://github.com/src-d/go-mysql-server/pull/820))
- Convert LIKE patterns to specific Go regexes ([#817](https://github.com/src-d/go-mysql-server/pull/817))

## [0.24.0-rc1] - 2019-09-19

### Added

- function: use new caches from go-mysql-server ([#957](https://github.com/src-d/gitbase/pull/957))

### Changed

- `go-borges` library was updated to `0.1.3`:
  - support metadata reloading [issue](https://github.com/src-d/go-borges/pull/84)
  - speedup loading legacy sivas with lots of references [issue](https://github.com/src-d/go-borges/pull/87)
  - fix bug that misplaced location metadata [issue](https://github.com/src-d/go-borges/pull/88)

## [0.24.0-beta3] - 2019-07-31

### Added

- `uast_imports` function to gather import paths from an UAST.
- sql: implement memory management system for caches  ([#802](https://github.com/src-d/go-mysql-server/pull/802))
- function: implement regexp_matches ([#794](https://github.com/src-d/go-mysql-server/pull/794))

### Fixed

- Added a checker that will detect dead sockets before the timeout (Linux only)
- Make Sleep check for cancelled context every second ([#798](https://github.com/src-d/go-mysql-server/pull/798))

## [0.24.0-beta2] - 2019-07-31

### Changed

- When it's added the `-v` verbose flag, gitbase will use `debug` as logging level, ignoring any other passed ([#935](https://github.com/src-d/gitbase/pull/935))

### Fixed

- If using docker image, and `info` logging level, it will be now used instead of `debug` ([#935](https://github.com/src-d/gitbase/pull/935))
- sql/analyzer: fix order by resolution for all nodes ([#793](https://github.com/src-d/gitbase/pull/793))
- sql: fix SQL method for arrays of JSON ([#790](https://github.com/src-d/gitbase/pull/790))

## [0.24.0-beta1] - 2019-07-08

### Added

- Varchar type.
- FIRST and LAST aggregations.
- Count distinct aggregation.

### Changed

- Errors now report the repository causing the error, if possible.
- Switch some types of known or maximum length (mostly hashes and emails)
  to VarChar with a size.
- Traces now have a root span.
- New API for node transformations.

### Fixed

- Fixed the behaviour of limit and offset.
- Resolution of HAVING nodes.

## [0.23.1] - 2019-07-05

### Fixed

- Fix the results of files table by not using git log.

## [0.23.0] - 2019-07-04

### Changed

- Now non rooted siva files support old siva rooted repositories.

## [0.22.0] - 2019-07-03

### Added

- Now gitbase uses [go-borges](https://github.com/src-d/go-borges) to access repositories
  - The type of files in each directory has to be specified ([#867](https://github.com/src-d/gitbase/pull/867))
  - Supports new rooted repository format and separates references and objects from each repo (https://github.com/src-d/borges/issues/389)

### Changed

- Changed cli to be able to specify different formats ([#866](https://github.com/src-d/gitbase/issues/866))

### Fixed

- function: correctly transform up explode nodes ([#757](https://github.com/src-d/go-mysql-server/pull/757))
- git libraries bare or non bare format is automatically detected ([#897](https://github.com/src-d/gitbase/pull/897))
- Fix bug that created multiple object cache with incorrect size ([#898](https://github.com/src-d/gitbase/pull/898))
- sql/expression: handle null values in arithmetic expressions ([#760](https://github.com/src-d/go-mysql-server/pull/760))
- Panic on query using EXPLODE ([#755](https://github.com/src-d/go-mysql-server/issues/755))
- Fixed error iterating over non ready repositories ([src-d/go-borges#54](https://github.com/src-d/go-borges/pull/54))
- Error saying value could not be converted to bool.
- function: make array_length not fail with literal null ([#767](https://github.com/src-d/go-mysql-server/pull/767))
- server: kill queries on connection closed (([#769](https://github.com/src-d/go-mysql-server/pull/769)))

## [0.22.0-rc2] - 2019-06-24

### Fixed
- Panic on query using EXPLODE ([#755](https://github.com/src-d/go-mysql-server/issues/755))
- Fixed error iterating over non ready repositories ([src-d/go-borges#54](https://github.com/src-d/go-borges/pull/54))
- Error saying value could not be converted to bool.
- function: make array_length not fail with literal null ([#767](https://github.com/src-d/go-mysql-server/pull/767))

## [0.22.0-rc1] - 2019-06-21

### Added

- Now gitbase uses [go-borges](https://github.com/src-d/go-borges) to access repositories
  - The type of files in each directory has to be specified ([#867](https://github.com/src-d/gitbase/pull/867))
  - Supports new rooted repository format and separates references and objects from each repo (https://github.com/src-d/borges/issues/389)

### Changed

- Changed cli to be able to specify different formats ([#866](https://github.com/src-d/gitbase/issues/866))

### Fixed

- function: correctly transform up explode nodes ([#757](https://github.com/src-d/go-mysql-server/pull/757))
- git libraries bare or non bare format is automatically detected ([#897](https://github.com/src-d/gitbase/pull/897))
- Fix bug that created multiple object cache with incorrect size ([#898](https://github.com/src-d/gitbase/pull/898))
- sql/expression: handle null values in arithmetic expressions ([#760](https://github.com/src-d/go-mysql-server/pull/760))

## [0.22.0-beta1] - 2019-06-20

### Added

- Now gitbase uses [go-borges](https://github.com/src-d/go-borges) to access repositories
  - The type of files in each directory has to be specified ([#867](https://github.com/src-d/gitbase/pull/867))
  - Supports new rooted repository format and separates references and objects from each repo (https://github.com/src-d/borges/issues/389)

### Changed

- Changed cli to be able to specify different formats ([#866](https://github.com/src-d/gitbase/issues/866))

### Fixed

- function: correctly transform up explode nodes ([#757](https://github.com/src-d/go-mysql-server/pull/757))

## [0.21.0] - 2019-06-20

### Known bugs
- https://github.com/src-d/gitbase/issues/886

### Added
- Added `json_unquote` function.
- Added `commit_file_stats` function.
- Added documentation about `commit_stats`.
- Add metrics (engine, analyzer, regex, pilosa) based on go-kit interface. ([#744](https://github.com/src-d/go-mysql-server/pull/744))
- `commit_files` is now squashable with `blobs`.
- Moved to Go modules.
- Add COMMIT_STATS function
- sql: implement EXPLODE and generators ([#720](https://github.com/src-d/go-mysql-server/pull/720))

### Changed

- Removed vendor folder.
- Upgrade `enry` to version `v2.0.0`.
- Switch `gocloc` to version `v0.3.0`.
- Upgrade vitess to v1.8.0 ([#738](https://github.com/src-d/go-mysql-server/pull/738))
- Upgrade bblfsh to v4.1.0.
- Upgrade gocloc to latest master 764f3f6ae21e.

### Fixed

- bblfsh aliases are now handled correctly ([#728](https://github.com/src-d/gitbase/issues/728)).
- sql: correctly handle nulls in SQL type conversion ([#753](https://github.com/src-d/go-mysql-server/pull/753))
- sql/parse: error for unsupported distinct on aggregations ([#869](https://github.com/src-d/gitbase/issues/869))
- internal/function: gracefully handle errors in commit_stats.
- internal/function: take into account if repository is resolved in commit_stats ([#863](https://github.com/src-d/gitbase/pull/863))
- internal/function: `Files` field in `commit_stats` contains now proper results.
- Fix parsing of quoted identifiers in SHOW CREATE TABLE queries ([#737](https://github.com/src-d/go-mysql-server/pull/737))
- sql/analyzer: back-propagate expression names after adding convert ([#739](https://github.com/src-d/go-mysql-server/pull/739))
- sql/expression: allow null literals in case branches ([#741](https://github.com/src-d/go-mysql-server/pull/741))
- sql/plan: make LEFT and RIGHT join work as expected ([#743](https://github.com/src-d/go-mysql-server/pull/743))

## [0.21.0-beta3] - 2019-06-19

### Fixed

- bblfsh aliases are now handled correctly ([#728](https://github.com/src-d/gitbase/issues/728)).
- sql: correctly handle nulls in SQL type conversion ([#753](https://github.com/src-d/go-mysql-server/pull/753))
- sql/parse: error for unsupported distinct on aggregations ([#869](https://github.com/src-d/gitbase/issues/869))

## [0.21.0-beta2] - 2019-06-18

### Added

- Added `json_unquote` function.
- Added `commit_file_stats` function.
- Added documentation about `commit_stats`.
- `commit_files` is now squashable with `blobs`.
- Add metrics (engine, analyzer, uast, pilosa) based on go-kit interface. ([#744](https://github.com/src-d/go-mysql-server/pull/744)).
- Expose (if enabled) prometheus metrics over http (#815).

### Changed

- Removed vendor folder.
- Upgrade `enry` to version `v2.0.0`.
- Switch `gocloc` to version `v0.3.0`.

### Fixed

- internal/function: gracefully handle errors in commit_stats.
- internal/function: take into account if repository is resolved in commit_stats ([#863](https://github.com/src-d/gitbase/pull/863))
- internal/function: `Files` field in `commit_stats` contains now proper results.

## [0.21.0-beta1] - 2019-06-12

### Added
- Moved to Go modules.
- Add COMMIT_STATS function
- sql: implement EXPLODE and generators ([#720](https://github.com/src-d/go-mysql-server/pull/720))

### Changed
- Upgrade vitess to v1.8.0 ([#738](https://github.com/src-d/go-mysql-server/pull/738))
- Upgrade bblfsh to v4.1.0.
- Upgrade gocloc to latest master 764f3f6ae21e.

### Fixed
- Fix parsing of quoted identifiers in SHOW CREATE TABLE queries ([#737](https://github.com/src-d/go-mysql-server/pull/737))
- sql/analyzer: back-propagate expression names after adding convert ([#739](https://github.com/src-d/go-mysql-server/pull/739))
- sql/expression: allow null literals in case branches ([#741](https://github.com/src-d/go-mysql-server/pull/741))
- sql/plan: make LEFT and RIGHT join work as expected ([#743](https://github.com/src-d/go-mysql-server/pull/743))

## [0.20.0] - 2019-05-30
### Known Issues
- After updating Vitess MySQL server, we are having some problems connecting from JDBC MariaDB drivers (https://github.com/src-d/gitbase/issues/807) (https://github.com/vitessio/vitess/issues/4603)

### Added
- function: implement is_vendor function (#830)
- Suggest table/column/indexes names on missing errors
- sql: HAVING clause
- Support SHOW SCHEMAS (upgrade vitess to v1.6.0) (https://github.com/src-d/go-mysql-server/pull/696)
- function: LOC function implementation (#798)
- sql/expression: new DATE function
- sql: add support for intervals, DATE_SUB and DATE_ADD
- sql: from_base64 and to_base64 functions
- sql: add SLEEP function
- COUNT expression now returns an int64 number instead of int32 https://github.com/src-d/go-mysql-server/issues/642
- Dockerfile: include zero-config MySQL client https://github.com/src-d/gitbase/pull/737
- uast_extract function now returns a JSON for `pos` instead of a custom format https://github.com/src-d/gitbase/pull/715

#### Documentation
- docs: expand optimization guide on early filtering (#837)
- Now all relevant go-mysql-server documentation is directly accessible from gitbase docs instead of pointing to external links.
- Docs: document in-memory joins in optimization docs https://github.com/src-d/gitbase/pull/742

#### Performance
-  Make mapping per partition on Index creation, improving performance (https://github.com/src-d/go-mysql-server/pull/681).
- sql/index/pilosa: parallelize index creation
- Perf: improve the way we check if refs are not pointing to commits https://github.com/src-d/gitbase/pull/780
- Plan: compute all inner joins in memory if they fit https://github.com/src-d/go-mysql-server/issues/577
- Perf: Avoid call to Checksum if there are no indexes https://github.com/src-d/go-mysql-server/pull/631

### Changed
- COUNT expression is returning now int64 instead of int32
- uast_extract function now returns a JSON for `pos` instead of a custom format
- Now relative paths are used as repository_id instead of folder name

### Fixed
- avoid panic when there are no fetch URLs in remote config (#836)
- upgrade go-mysql-server and gocloc (#831)
- rule: fix squash rule with convert_dates
- cmd/server/commands: use relative path as id instead of last part (#816)
- handle backslashes correctly
- sql/plan: make sure outdated indexes can be dropped
- sql/analyzer: correctly qualify aliases with the same name as col
- Fix validation rule to detect tuples in projections or groupbys (https://github.com/src-d/go-mysql-server/pull/672)
- sql/analyzer: only check aliases to qualify in the topmost project (https://github.com/src-d/go-mysql-server/pull/690)
- Fix special case for aggregation in ORDER BY
- Try to order by function (https://github.com/src-d/go-mysql-server/pull/692)
- Don't skip repositories for remotes table with more than 1 URL https://github.com/src-d/gitbase/pull/789
- server: correctly set binary charset on blob fields
- sql/parse: allow qualified table names on SHOW CREATE TABLE
- plan: types in lowercase on SHOW CREATE TABLE
- Skip a directory if gitbase has no permission to read it https://github.com/src-d/gitbase/pull/738
- Close iterators correctly to avoid too many open files error https://github.com/src-d/gitbase/pull/772
- Check projection aliases when assigned to index https://github.com/src-d/go-mysql-server/issues/639
- Add charset to fields to avoid invalid column types when using JDBC clients https://github.com/src-d/go-mysql-server/pull/637
- Fix prune columns for describe queries https://github.com/src-d/go-mysql-server/pull/634
- Allow all expressions in grouping, resolve order by expressions https://github.com/src-d/go-mysql-server/pull/633
- KILL query always takes processlist_id https://github.com/src-d/go-mysql-server/pull/636
- Recover panic for partitions https://github.com/src-d/go-mysql-server/pull/626
