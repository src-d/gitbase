# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Move to go modules and upgrade bblfsh client version.

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
