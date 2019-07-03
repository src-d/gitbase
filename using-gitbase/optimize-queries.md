# Optimizing queries

Even though in each release performance improvements are included to make gitbase faster, there are some queries that might take too long. By rewriting them in some ways, you can squeeze that extra performance you need by taking advantage of some optimisations that are already in place.

There are two ways to optimize a gitbase query:

* Create an index for some parts.
* Making sure the joined tables are squashed.
* Making sure not squashed joins are performed in memory.

## Assessing performance bottlenecks

To assess if there is a performance bottleneck you might want to inspect the execution tree of the query. This is also very helpful when reporting performance issues on gitbase.

The output from an `EXPLAIN` query is represented as a tree and shows how the query is actually evaluated. You can do that using the following query:

```sql
EXPLAIN FORMAT=TREE <SQL QUERY TO EXPLAIN>
```

For example, the given query:

```sql
EXPLAIN FORMAT=TREE
    SELECT * FROM refs
    NATURAL JOIN ref_commits
    WHERE ref_commits.history_index = 0
```

Will output something like this:

```text
+-----------------------------------------------------------------------------------------+
| plan                                                                                    |
+-----------------------------------------------------------------------------------------+
| Project(refs.repository_id, refs.ref_name, refs.commit_hash, ref_commits.history_index) |
|  └─ SquashedTable(refs, ref_commits)                                                    |
|      ├─ Columns                                                                         |
|      │   ├─ Column(repository_id, TEXT, nullable=false)                                 |
|      │   ├─ Column(ref_name, TEXT, nullable=false)                                      |
|      │   ├─ Column(commit_hash, TEXT, nullable=false)                                   |
|      │   ├─ Column(repository_id, TEXT, nullable=false)                                 |
|      │   ├─ Column(commit_hash, TEXT, nullable=false)                                   |
|      │   ├─ Column(ref_name, TEXT, nullable=false)                                      |
|      │   └─ Column(history_index, INT64, nullable=false)                                |
|      └─ Filters                                                                         |
|          ├─ refs.repository_id = ref_commits.repository_id                              |
|          ├─ refs.ref_name = ref_commits.ref_name                                        |
|          ├─ refs.commit_hash = ref_commits.commit_hash                                  |
|          └─ ref_commits.history_index = 0                                               |
+-----------------------------------------------------------------------------------------+
15 rows in set (0.00 sec)
```

#### Detecting performance issues in the query tree

Some performance issues might not be obvious, but there are a few that really stand out by just looking at the query tree.

* Joins not squashed. If you performed some joins between tables and instead of a `SquashedTable` node you see `Join` and `Table` nodes, it means the joins were not successfully squashed. There is a more detailed explanation about this in next sections of this document.
* Indexes not used. If you can't see the indexes in your table nodes, it means somehow those indexes are not being used by the table. There is a more detailed explanation about this in next sections of this document.
* Joins not squashed that are not being executed in memory. There is a more detailed explanation about this in the next sections of this document.

## In-memory joins

There are two modes in which gitbase can execute an inner join:

* Multipass: it fully iterates the right side of the join one time for each row in the left side. This is really expensive, but avoids having to load one side fully in memory.
* In-memory: loads the whole right side in memory and iterates the left side. Both sides are iterated exactly once, thus it makes the query much faster, but it has the disadvantage of potentially requiring a lot of memory.

The default mode is multipass, unless the right side fits in memory \(there's a more elaborate explanation about this below\).

In-memory joins can be enabled at the user request, either with the `EXPERIMENTAL_IN_MEMORY_JOIN=on` environment variable or executing `SET inmemory_joins = 1`. The last method only enables it for the current connection.

Even if they are not globally enabled for all queries, there is an optimization that checks if the join could be performed in memory and if it can't, switches to multipass mode. As long as the whole gitbase server memory usage is under the 20% of all available physical \(not counting other memory used by other processes\) memory in the machine, the join will be performed in memory. When this limit is passed, the multipass mode will be used instead. 20% is just a default value that can be changed using the `MAX_MEMORY_INNER_JOIN` environment variable to the maximum amount of bytes the gitbase server can be using before switching to multipass mode. It can also be changed per session using `SET max_memory_joins=<MAX BYTES>`.

So, as a good rule of thumb, the right side of an inner join should always be the smaller one, because that way, it has bigger chances of being executed in memory and it will be faster.

## Indexes

The more obvious way to improve the performance of a query is to create an index for such query. Since you can index multiple columns or a single arbitrary expression, this may be useful for some kinds of queries. For example, if you're querying by language, you may want to index that so there is no need to compute the language each time.

```sql
CREATE INDEX files_language_idx ON files USING pilosa (language(file_path, blob_content))
```

Once you have the index in place, gitbase only looks for the rows with the values matching your conditions.

But beware, even if you have an index it's possible that gitbase will not use it. These are the forms an expression **must** have to make sure the index will be used.

* `<indexed expression> = <evaluable expression>`
* `<indexed expression> < <evaluable expression>`
* `<indexed expression> > <evaluable expression>`
* `<indexed expression> <= <evaluable expression>`
* `<indexed expression> >= <evaluable expression>`
* `<indexed expression> != <evaluable expression>`
* `<indexed expression> IN <evaluable expression>`
* `<indexed expression> BETWEEN <evaluable expression> AND <evaluable expression>`

`<indexed expression>` is the expression that was indexed when the index was created, in the previous case that would be `language(file_path, blob_content)`. `<evaluable expression>` is any expression that can be evaluated without using the current row. For example, a literal \(`"foo"`\), a function that takes no column arguments \(`SUBSTRING("foo", 1)`\), etc.

So, if you have this query, the index would be used.

```sql
SELECT file_path FROM files WHERE language(file_path, blob_content) = 'Go'
```

But these queries would not use the index.

```sql
SELECT file_path FROM files WHERE language(file_path, blob_content) = SUBSTRING(file_path, 0, 2)
```

```sql
SELECT file_path FROM files WHERE language(file_path, blob_content) LIKE 'G_'
```

Note that when you use an index on multiple columns, there is a limitation \(that may change in the future\) that requires all columns sharing the same operation.

For example, let's make an index on two columns.

```sql
CREATE INDEX commits_multi_idx ON commits USING pilosa (committer_name, committer_email)
```

This query would use the index.

```sql
SELECT * FROM commits WHERE committer_name = 'John Doe' AND committer_email = 'foo@example.com'
```

These, however, would not use the index.

```sql
SELECT * FROM commits WHERE committer_name = 'John Doe'
```

All columns in an index need to be present in the filters.

```sql
SELECT * FROM commits WHERE committer_name = 'John Doe' AND committer_email != 'foo@example.com'
```

All the columns need to use the same operation. In this case, one is using `=` and the other `!=`. This is a current limitation that will be removed in the future.

## Squash tables

There is an optimization done inside gitbase called **squashed tables**. Instead of reading all the data from the tables and then performing the join, a squashed table is the union of several tables in which the output of a table is generated using the output of the previous one.

Imagine we want to join `commits`, `commit_files` and `files`. Without the squashed joins we would read all `commits`, all `commit_files` and all `files`. Then, we would join all these rows. This is an incredibly expensive operation for large repositories. With squashed tables, however, we read all `commits`, then, for each commit we generate the `commit_files` for that commit and then for each commit file we generate the `files` for them. This has two advantages:

* Filters are applied early on, which reduces the amount of data that needs to be read. If you filtered commits by a particular author in our previous example, only commit files, and thus files, by that commit author would be read, instead of all of them.
* It works with raw git objects, not database rows, which makes it way more performant since there is no need to serialize and deserialize.

As a result, your query could be orders of magnitude faster.

#### Squashed table optimizations

In squashed tables, data flows from the topmost table in terms of hierarchy towards the rest of the tables. That way, if a squashed table is made of `repositories`, `commits` and `commit_files` the process to generate the data is the following:

1. Get a repository. If there are no more repositories, finish.
2. If it satisfies the filters given to the `repositories` table go to step 3, otherwise, go to step 1 again.
3. Get the next commit for the current repository. If there are no more commits for this repository, go to 1 again.
4. If it satisfies the filters given to the `commits` table go to step 4, otherwise, go to step 3 again.
5. Get the next commit file for the current commit. If there are no more commit files for this commit, go to 3 again.
6. If it satisfies the filters given to the `commits_files` table return the composed row, otherwise, go to step 5 again.

This way, the less data coming from the upper table, the less work the next table will have to do, and thus, the faster it will be. A good rule of thumb is to apply a filter as soon as possible. That is, if there is a filter by `repository_id` it's better to do `repositories.repository_id = 'SOME REPO'` than `commits.repository_id = 'SOME_REPO'`. Because even if the result will be the same, it will avoid doing a lot of useless computing for the repositories that do not satisfy that filter.

To illustrate this, let's consider the following example:

We have 2 repositories, `A` and `B`. Each repository has 3 commits.

With this query we will get the three commits from `A`.

```sql
SELECT * FROM repositories NATURAL JOIN commits WHERE commits.repository_id = 'A'
```

But we have processed `B`'s commits as well, because the filter is done in commits. 2 repositories make it to the `commits` table, and then it generates 6 rows, 3 of which make it past the filters, resulting in 3 rows.

With this query we will get the three commits from `A` as well.

```sql
SELECT * FROM repositories NATURAL JOIN commits WHERE repositories.repository_id = 'A'
```

However, this time, 1 repository makes it past the filters in the `repositories` table and is sent to the `commits` table, and then it generates 3 rows, resulting in 3 rows.

The results are the same but we have reduced significantly the amount of computing needed for this query. Now consider having 1000 repositories with 1M commits each. Both of these queries would be generating 1M rows. The difference is the first one would be computing 1B rows, and the second only 1M.

This advice can be applied to all squashed tables, not only `repository_id`.

#### Limitations

**Only works per repository**. This optimisation is built on top of some premises, one of them is the fact that all tables are joined by `repository_id`.

This query will get squashed, because `NATURAL JOIN` makes sure all columns with equal names are used in the join.

```sql
SELECT * FROM refs NATURAL JOIN ref_commits NATURAL JOIN commits
```

This query, however, will not be squashed.

```sql
SELECT * FROM refs r
INNER JOIN ref_commits rc ON r.ref_name = rc.ref_name
INNER JOIN commits c ON rc.commit_hash = c.commit_hash
```

**It requires some filters to be present in order to perform the squash.**

This query will be squashed.

```sql
SELECT * FROM commit_files NATURAL JOIN files
```

This query will not be squashed, as the join between `commit_files` and `files` requires more filters to be squashed.

```sql
SELECT * FROM commit_files cf
INNER JOIN files f ON cf.file_path = f.file_path
```

**TIP:** we suggest always using `NATURAL JOIN` for joining tables, since it's less verbose and already satisfies all the filters for squashing tables. The only exception to this advice is when joining `refs` and `ref_commits`. A `NATURAL JOIN` between `refs` and `ref_commits` will only get the HEAD commit of the reference. The same happens with `commits` and `commit_trees`/`commit_files`.

You can find the full list of conditions that need to be met for the squash to be applied [here](optimize-queries.md#list-of-filters-for-squashed-tables).

**Only works if the tables joined follow a hierarchy.** Joinin `commits` and `files` does not work, or joining `blobs` with `files`. It needs to follow one of the hierarchies of tables.

```text
repositories -> refs -> ref_commits -> commits -> commit_trees -> tree_entries -> blobs
repositories -> refs -> ref_commits -> commits -> commit_blobs -> blobs
repositories -> refs -> ref_commits -> commits -> commit_files -> blobs
repositories -> refs -> ref_commits -> commits -> commit_files -> files
repositories -> remotes -> refs -> (any of the other hierarchies)
```

As long as the tables you join are a subset of any of these hierarchies, it will be applied, provided you gave the proper filters. If only some part follows the hierarchy, the leftmost squash will be performed.

For example, if we join `repositories`, `remotes`, and then `commit_blobs` and `blobs`, the result will be a squashed table of `repositories` and `remotes` and a regular join with `commit_blobs` and `blobs`. The rule will try to squash as many tables as possible.

### How to check if the squash was applied

You can check if the squash optimisation was applied to your query by using the `DESCRIBE` command.

```sql
DESCRIBE FORMAT=TREE <your query>
```

This will pretty-print the analyzed tree of your query. If you see a node named `SquashedTable` it means your query was squashed, otherwise some part of your query is not squashable or a filter might be missing.

### List of filters for squashed tables

`T1.repository_id = T2.repository_id`: all tables must be joined by `repository_id`.

#### `refs` with `ref_commits`

* `refs.ref_name = ref_commits.ref_name`
* `refs.commit_hash = ref_commits.commit_hash` \(only if you want to get just the HEAD commit\)

#### `refs` with `commits`

* `refs.commit_hash = commits.commit_hash`

#### `refs` with `commit_trees`

* `refs.commit_hash = commit_trees.commit_hash`

#### `refs` with `commit_blobs`

* `refs.commit_hash = commit_blobs.commit_hash`

#### `refs` with `commit_files`

* `refs.commit_hash = commit_files.commit_hash`

#### `ref_commits` with `commits`

* `ref_commits.commit_hash = commits.commit_hash`

#### `ref_commits` with `commit_trees`

* `ref_commits.commit_hash = commit_trees.commit_hash`

#### `ref_commits` with `commit_blobs`

* `ref_commits.commit_hash = commit_blobs.commit_hash`

#### `ref_commits` with `commit_files`

* `ref_commits.commit_hash = commit_files.commit_hash`
* `commits.tree_hash = commit_files.tree_hash` \(only if you want just the main commit tree files\)

#### `commits` with `commit_trees`

* `commits.commit_hash = commit_trees.commit_hash`
* `commits.tree_hash = commit_trees.tree_hash` \(only if you want just the main commit tree\)

#### `commits` with `commit_blobs`

* `commits.commit_hash = commit_blobs.commit_hash`

#### `commits` with `commit_files`

* `commits.commit_hash = commit_files.commit_hash`

### `commits` with `tree_entries`

* `commits.tree_hash = tree_entries.tree_hash`

### `commit_trees` with `tree_entries`

* `commit_trees.tree_hash = tree_entries.tree_hash`

### `commit_blobs` with `blobs`

* `commit_blobs.blob_hash = blobs.blob_hash`

### `tree_entries` with `blobs`

* `tree_entries.blob_hash = blobs.blob_hash`

### `commit_files` with `blobs`

* `commit_files.blob_hash = blobs.blob_hash`

### `commit_files` with `files`

* `commit_files.file_path = files.file_path`
* `commit_files.tree_hash = files.tree_hash`
* `commit_files.blob_hash = files.blob_hash`

## GROUP BY and ORDER BY memory optimization

The way GROUP BY and ORDER BY are implemented, they hold all the rows their child node will return in memory and once all of them are present, the grouping/sort is computed. In order to optimise a query having an ORDER BY or GROUP BY is important to perform those operations as late as possible and with the least amount of data possible. Otherwise, they can have a very big impact on memory usage and performance.

For example, consider the following query:

```sql
SELECT LANGUAGE(f.file_path) as lang, SUM(ARRAY_LENGTH(SPLIT(f.blob_content, "\n"))-1) as lines
FROM ref_commits rc
NATURAL JOIN commits c
NATURAL JOIN commit_files cf
NATURAL JOIN files f
WHERE rc.ref_name = 'HEAD'
    AND f.file_path NOT REGEXP '^vendor.*'
    AND NOT IS_BINARY(f.blob_content)
GROUP BY lang
```

This query returns the total number of lines of code per language in all files in the HEAD reference of all repositories. What happens here is that grouping will be done with a row that contains `blob_content`. This means a lot of data will be kept in memory to perform this aggregation. That could lead to tens of gigabytes of RAM usage if there are a lot of repositories in the dataset.

Instead, the following query returns exactly the same rows, but only outputs what's necessary in a subquery, keeping way less data in memory.

```sql
SELECT lang, SUM(lines) AS lines
FROM (
    SELECT LANGUAGE(f.file_path, f.blob_content) as lang,
        (ARRAY_LENGTH(SPLIT(f.blob_content, "\n"))-1) as lines
    FROM ref_commits rc
    NATURAL JOIN commits c
    NATURAL JOIN commit_files cf
    NATURAL JOIN files f
    WHERE rc.ref_name = 'HEAD'
        AND cf.file_path NOT REGEXP '^vendor.*'
        AND NOT IS_BINARY(f.blob_content)
) t
GROUP BY lang
```

As a good rule of thumb: defer as much as possible GROUP BY and ORDER BY operations and only perform them with the minimum amount of data needed.

