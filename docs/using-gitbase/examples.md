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

## Files in the first commit on HEAD history for all repositories

```sql
SELECT file_path,
       ref_commits.repository_id
FROM commit_files
NATURAL JOIN ref_commits
WHERE ref_name = 'HEAD'
  AND history_index = 0;
```

## Commits that appear in more than one reference

```sql
SELECT COUNT(commit_hash) AS num, commit_hash
FROM ref_commits
GROUP BY commit_hash
HAVING num > 1;
```

## Get the number of blobs per HEAD commit

```sql
SELECT COUNT(blob_hash),
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

## Report of line count per file from HEAD references

```sql
SELECT
    LANGUAGE(file_path, blob_content) as lang,
    SUM(JSON_EXTRACT(LOC(file_path, blob_content), '$.Code')) as code,
    SUM(JSON_EXTRACT(LOC(file_path, blob_content), '$.Comments')) as comments,
    SUM(JSON_EXTRACT(LOC(file_path, blob_content), '$.Blanks')) as blanks,
    COUNT(1) as files
FROM refs
NATURAL JOIN commit_files
NATURAL JOIN blobs
WHERE ref_name='HEAD'
GROUP BY lang;
```

## Files from first 6 commits from HEAD references that contains some key and are not in vendor directory

```sql
SELECT file_path,
       repository_id,
       blob_content
FROM ref_commits
NATURAL JOIN commit_files
NATURAL JOIN files
WHERE ref_name = 'HEAD'
  AND history_index BETWEEN 0 AND 5
  AND NOT IS_BINARY(blob_content)
  AND NOT IS_VENDOR(file_path)
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

## Calculating code line changes in the last commit

This query will report how many lines of actual code (only code, not comments, blank lines or text) changed in the last commit of each repository.

```sql
SELECT
    repo,
    JSON_EXTRACT(stats, '$.Code.Additions') AS code_lines_added,
    JSON_EXTRACT(stats, '$.Code.Deletions') AS code_lines_removed
FROM (
    SELECT
        repository_id AS repo,
        COMMIT_STATS(repository_id, commit_hash) AS stats
    FROM refs
    WHERE ref_name = 'HEAD'
) t;
```

The output will be similar to this:

```
+-----------------+------------------+--------------------+
| repo            | code_lines_added | code_lines_removed |
+-----------------+------------------+--------------------+
| salty-wombat    | 56               | 2                  |
| sugar-boogaloo  | 11               | 1                  |
+-----------------+------------------+--------------------+
```

## Calculating code line changes for files in the last commit

This query will report how many lines of actual code (only code, not comments, blank lines or text) changed in each file of the last commit of each repository. It's similar to the previous example. `COMMIT_STATS` is an aggregation over the result of `COMMIT_FILE_STATS` so to speak.
We will only report those files whose language has been identified.

```sql
SELECT
    repo,
    JSON_UNQUOTE(JSON_EXTRACT(stats, '$.Path')) AS file_path,
    JSON_UNQUOTE(JSON_EXTRACT(stats, '$.Language')) AS file_language,
    JSON_EXTRACT(stats, '$.Code.Additions') AS code_lines_added,
    JSON_EXTRACT(stats, '$.Code.Deletions') AS code_lines_removed
FROM (
    SELECT
        repository_id AS repo,
        EXPLODE(COMMIT_FILE_STATS(repository_id, commit_hash)) AS stats
    FROM refs
    WHERE ref_name = 'HEAD'
) t
WHERE file_language <> '';
```

The output will be similar to this:

```
+-----------------+--------------------------------------+---------------+------------------+--------------------+
| repo            | file_path                            | file_language | code_lines_added | code_lines_removed |
+-----------------+--------------------------------------+---------------+------------------+--------------------+
| salty-wombat    | main.py                              | Python        | 40               | 0                  |
| salty-wombat    | __init__.py                          | Python        | 16               | 2                  |
| sugar-boogaloo  | server.go                            | Go            | 11               | 1                  |
+-----------------+--------------------------------------+---------------+------------------+--------------------+
```

# UAST UDFs Examples

First of all, you should check out the [bblfsh documentation](https://docs.sourced.tech/babelfish) to get yourself familiar with UAST concepts.

Also, you can take a look to all the UDFs and their signatures in the [functions section](/docs/using-gitbase/functions.md)

## Extracting all identifier names

```sql
SELECT file_path,
       uast_extract(uast(blob_content, LANGUAGE(file_path), '//uast:Identifier'), "Name") name
FROM refs
NATURAL JOIN commit_files
NATURAL JOIN blobs
WHERE ref_name = 'HEAD' AND LANGUAGE(file_path) = 'Go';
```

As result, you will get an array showing a list of the retrieved information. Each element in the list matches a node in the given sequence of nodes having a value for that property. It means that the length of the properties list may not be equal to the length of the given sequence of nodes:

```sh
+-------------------------------------------------------------------------------------------------------------------+
| file_path        | name                                                                                           |
+-------------------+-----------------------------------------------------------------------------------------------+
| _example/main.go | ["main","driver","NewDefault","sqle","createTestDatabase","AddDatabase","driver","auth"]       |
+-------------------+-----------------------------------------------------------------------------------------------+
```

## Extracting all import paths

```sql
SELECT file_path,
       uast_imports(uast(blob_content, LANGUAGE(file_path, blob_content))) AS imports
FROM refs
NATURAL JOIN commit_files
NATURAL JOIN blobs
WHERE ref_name = 'HEAD' AND LANGUAGE(file_path) = 'Go';
```

As result, you will get an array with an array of import paths for each node in the UAST.

```sh
+-------------------------------------------------------------------------------------------------------------------+
| file_path        | imports                                                                                        |
+-------------------+-----------------------------------------------------------------------------------------------+
| _example/main.go | [["fmt","database/sql","github.com/sirupsen/logrus"]]                                          |
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
- It's querying `commit_files` table and has processed 8 out of 9 partitions.

To kill a query that's currently running you can use the value in `Id`. If we were to kill the previous query, we would need to use the following query:

```sql
KILL QUERY 168;
```
