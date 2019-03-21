# Examples

## Get all the repositories where a specific user contributes on HEAD reference

```sql
SELECT refs.repository_id
FROM refs
NATURAL JOIN commits
WHERE commits.commit_author_name = 'Johnny Bravo'
  AND refs.ref_name = 'HEAD';
```

## Get all the HEAD references from all the repositories

```sql
SELECT *
FROM refs
WHERE ref_name = 'HEAD';
```

## First commit on HEAD history for all repositories

```sql
SELECT file_path,
       ref_commits.repository_id
FROM commit_files
NATURAL JOIN ref_commits
WHERE ref_commits.ref_name = 'HEAD'
  AND ref_commits.history_index = 0;
```

## Commits that appear in more than one reference

```sql
SELECT *
FROM
  (SELECT COUNT(c.commit_hash) AS num,
          c.commit_hash
   FROM ref_commits r
   NATURAL JOIN commits c
   GROUP BY c.commit_hash) t
WHERE num > 1;
```

##  Get the number of blobs per HEAD commit

```sql
SELECT COUNT(commit_hash),
       commit_hash
FROM ref_commits
NATURAL JOIN commits
NATURAL JOIN commit_blobs
WHERE ref_name = 'HEAD'
GROUP BY commit_hash;
```

## Get commits per committer, per year and month

```sql
SELECT YEAR,
       MONTH,
       repo_id,
       committer_email,
       COUNT(*) AS num_commits
FROM
  (SELECT YEAR(committer_when) AS YEAR,
          MONTH(committer_when) AS MONTH,
          repository_id AS repo_id,
          committer_email
   FROM ref_commits
   NATURAL JOIN commits
   WHERE ref_name = 'HEAD') AS t
GROUP BY committer_email,
         YEAR,
         MONTH,
         repo_id;
```

## Files from first 6 commits from HEAD references that contains some key and are not in vendor directory

```sql
SELECT file_path,
       repository_id,
       blob_content
FROM files
NATURAL JOIN commit_files
NATURAL JOIN ref_commits
WHERE ref_name = 'HEAD'
  AND ref_commits.history_index BETWEEN 0 AND 5
  AND is_binary(blob_content) = FALSE
  AND files.file_path NOT REGEXP '^vendor.*'
  AND (blob_content REGEXP '(?i)facebook.*[\'\\"][0-9a-f]{32}[\'\\"]'
       OR blob_content REGEXP '(?i)twitter.*[\'\\"][0-9a-zA-Z]{35,44}[\'\\"]'
       OR blob_content REGEXP '(?i)github.*[\'\\"][0-9a-zA-Z]{35,40}[\'\\"]'
       OR blob_content REGEXP 'AKIA[0-9A-Z]{16}'
       OR blob_content REGEXP '(?i)reddit.*[\'\\"][0-9a-zA-Z]{14}[\'\\"]'
       OR blob_content REGEXP '(?i)heroku.*[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}'
       OR blob_content REGEXP '.*-----BEGIN PRIVATE KEY-----.*'
       OR blob_content REGEXP '.*-----BEGIN RSA PRIVATE KEY-----.*'
       OR blob_content REGEXP '.*-----BEGIN DSA PRIVATE KEY-----.*'
       OR blob_content REGEXP '.*-----BEGIN OPENSSH PRIVATE KEY-----.*');
```

## Create an index for columns on a table

You can create an index either on a specific column or on several columns:

```sql
CREATE INDEX commits_hash_idx ON commits USING pilosa (commit_hash);

CREATE INDEX files_commit_path_blob_idx ON commit_files USING pilosa (commit_hash, file_path, blob_hash);
```

## Create an index for an expression on a table

Note that just one expression at a time is allowed to be indexed.

```sql
CREATE INDEX files_lang_idx ON files USING pilosa (language(file_path, blob_content));
```

## Drop a table's index

```sql
DROP INDEX files_lang_idx ON files;
```

# UAST UDFs Examples

First of all, you should check out the [bblfsh documentation](https://docs.sourced.tech/babelfish) to get yourself familiar with UAST concepts.

Also, you can take a look to all the UDFs and their signatures in the [functions section](/docs/using-gitbase/functions.md)

## Extract all import paths for every *Go* file on *HEAD* reference

```sql
SELECT repository_id,
       file_path,
       uast_extract(uast(blob_content, LANGUAGE(file_path), '//uast:Import/Path'), "Value") AS imports
FROM commit_files
NATURAL JOIN refs
NATURAL JOIN blobs
WHERE ref_name = 'HEAD'
  AND LANGUAGE(file_path) = 'Go'
  AND ARRAY_LENGTH(imports) > 0;
```

## Extracting all identifier names

```sql
SELECT file_path,
       uast_extract(uast(blob_content, LANGUAGE(file_path), '//uast:Identifier'), "Name") name
FROM commit_files
NATURAL JOIN refs
NATURAL JOIN blobs
WHERE ref_name='HEAD' AND LANGUAGE(file_path) = 'Go';
```

As result, you will get an array showing a list of the retrieved information. Each element in the list matches a node in the given sequence of nodes having a value for that property. It means that the length of the properties list may not be equal to the length of the given sequence of nodes:

```sh
+-------------------------------------------------------------------------------------------------------------------+
| file_path        | name                                                                                           |
+-------------------+-----------------------------------------------------------------------------------------------+
| _example/main.go | ["main","driver","NewDefault","sqle","createTestDatabase","AddDatabase","driver","auth"]       |
+-------------------+-----------------------------------------------------------------------------------------------+
```

## Monitor the progress of a query

You can monitor the progress of a gitbase query (either a regular query or an index creation query using `SHOW PROCESSLIST`).

Let's say we do the following query over a huge repository:

```sql
SELECT file_path, LANGUAGE(file_path) lang FROM commit_files;
```

With this query we can monitor its progress:

```sql
SHOW PROCESSLIST;
```

We'll get the following output:

```
+-----+------+-----------------+---------+---------+------+-------------------+--------------------------------------------------------------+
| Id  | User | Host            | db      | Command | Time | State             | Info                                                         |
+-----+------+-----------------+---------+---------+------+-------------------+--------------------------------------------------------------+
| 168 | root | 127.0.0.1:53514 | gitbase | query   |   36 | commit_files(8/9) | SELECT file_path, LANGUAGE(file_path) lang FROM commit_files |
| 169 | root | 127.0.0.1:53514 | gitbase | query   |    0 | running           | show processlist                                             |
+-----+------+-----------------+---------+---------+------+-------------------+--------------------------------------------------------------+
```

From this output, we can obtain some information about our query:
- It's been running for 36 seconds.
- It's querying commit_files table and has processed 8 out of 9 partitions.

To kill a query that's currently running you can use the value in `Id`. If we were to kill the previous query, we would need to use the following query:

```sql
KILL QUERY 168;
```