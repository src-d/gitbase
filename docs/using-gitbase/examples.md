# Examples

## Get all the repositories where a specific user contributes on HEAD reference

```sql
SELECT refs.repository_id
FROM refs
NATURAL JOIN commits
WHERE commits.commit_author_name = 'Javi Fontan' AND refs.ref_name='HEAD';
```

## Get all the HEAD references from all the repositories

```sql
SELECT * FROM refs WHERE ref_name = 'HEAD';
```

## First commit on HEAD history for all repositories

```sql
SELECT
	file_path,
	ref_commits.repository_id
FROM
	commit_files
NATURAL JOIN
	ref_commits
WHERE
	ref_commits.ref_name = 'HEAD'
	AND ref_commits.history_index = 0;
```

## Commits that appear in more than one reference

```sql
SELECT * FROM (
    SELECT COUNT(c.commit_hash) AS num, c.commit_hash
    FROM ref_commits r
    INNER JOIN commits c
        ON r.commit_hash = c.commit_hash
    GROUP BY c.commit_hash
) t WHERE num > 1;
```

##  Get the number of blobs per HEAD commit

```sql
SELECT COUNT(c.commit_hash), c.commit_hash
FROM ref_commits as r
INNER JOIN commits c
    ON r.ref_name = 'HEAD' AND r.commit_hash = c.commit_hash
INNER JOIN commit_blobs cb
    ON cb.commit_hash = c.commit_hash
GROUP BY c.commit_hash;
```

## Get commits per committer, per month in 2015

```sql
SELECT COUNT(*) as num_commits, month, repo_id, committer_email
FROM (
    SELECT
        MONTH(committer_when) as month,
        r.repository_id as repo_id,
        committer_email
    FROM ref_commits r
    INNER JOIN commits c
            ON YEAR(c.committer_when) = 2015 AND r.commit_hash = c.commit_hash
    WHERE r.ref_name = 'HEAD'
) as t
GROUP BY committer_email, month, repo_id;
```

## Files from first 6 commits from HEAD references that contains some key and are not in vendor directory

```sql
select
	files.file_path,
	ref_commits.repository_id,
    files.blob_content
FROM
	files
NATURAL JOIN
	commit_files
NATURAL JOIN
	ref_commits
WHERE
	ref_commits.ref_name = 'HEAD'
	AND ref_commits.history_index BETWEEN 0 AND 5
	AND is_binary(blob_content) = false
    AND files.file_path NOT REGEXP '^vendor.*'
	AND (
        blob_content REGEXP '(?i)facebook.*[\'\\"][0-9a-f]{32}[\'\\"]'
        OR blob_content REGEXP '(?i)twitter.*[\'\\"][0-9a-zA-Z]{35,44}[\'\\"]'
        OR blob_content REGEXP '(?i)github.*[\'\\"][0-9a-zA-Z]{35,40}[\'\\"]'
        OR blob_content REGEXP 'AKIA[0-9A-Z]{16}'
        OR blob_content REGEXP '(?i)reddit.*[\'\\"][0-9a-zA-Z]{14}[\'\\"]'
        OR blob_content REGEXP '(?i)heroku.*[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}'
        OR blob_content REGEXP '.*-----BEGIN PRIVATE KEY-----.*'
        OR blob_content REGEXP '.*-----BEGIN RSA PRIVATE KEY-----.*'
        OR blob_content REGEXP '.*-----BEGIN DSA PRIVATE KEY-----.*'
        OR blob_content REGEXP '.*-----BEGIN OPENSSH PRIVATE KEY-----.*'
    );
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
