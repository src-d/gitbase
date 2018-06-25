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
SELECT * FROM refs WHERE ref_name = 'HEAD'
```

## Commits that appears in more than one reference

```sql
SELECT * FROM (
    SELECT COUNT(c.commit_hash) AS num, c.commit_hash
    FROM ref_commits r
    INNER JOIN commits c
        ON r.commit_hash = c.commit_hash
    GROUP BY c.commit_hash
) t WHERE num > 1
```

##  Get the number of blobs per HEAD commit

```sql
SELECT COUNT(c.commit_hash), c.commit_hash
FROM ref_commits as r
INNER JOIN commits c
    ON r.ref_name = 'HEAD' AND r.commit_hash = c.commit_hash
INNER JOIN commit_blobs cb
    ON cb.commit_hash = c.commit_hash
GROUP BY c.commit_hash
```

## Get commits per commiter, per month in 2015

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
GROUP BY committer_email, month, repo_id
```
