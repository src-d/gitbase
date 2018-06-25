# Schema

You can execute the `SHOW TABLES` statement to get a list of the available tables.
To get all the columns and types of a specific table, you can write `DESCRIBE TABLE [tablename]`.

gitbase exposes the following tables:

## Main tables

### repositories
``` sql
+---------------+------+
| name          | type |
+---------------+------+
| repository_id | TEXT |
+---------------+------+
```

Table that contains all the repositories on the dataset. `repository_id` is the path to the repository folder.

In case of [siva files](https://github.com/src-d/go-siva/), the id is the path + the siva file name.

### remotes
``` sql
+----------------------+------+
| name                 | type |
+----------------------+------+
| repository_id        | TEXT |
| remote_name          | TEXT |
| remote_push_url      | TEXT |
| remote_fetch_url     | TEXT |
| remote_push_refspec  | TEXT |
| remote_fetch_refspec | TEXT |
+----------------------+------+
```

This table will return all the [remotes](https://git-scm.com/book/en/v2/Git-Basics-Working-with-Remotes) configured on git `config` file of all the repositories.

### refs
``` sql
+---------------+------+
| name          | type |
+---------------+------+
| repository_id | TEXT |
| ref_name      | TEXT |
| commit_hash   | TEXT |
+---------------+------+
```
This table contains all hash [git references](https://git-scm.com/book/en/v2/Git-Internals-Git-References) and the symbolic reference `HEAD` from all the repositories.

### commits
``` sql
+---------------------+-----------+
| name                | type      |
+---------------------+-----------+
| repository_id       | TEXT      |
| commit_hash         | TEXT      |
| commit_author_name  | TEXT      |
| commit_author_email | TEXT      |
| commit_author_when  | TIMESTAMP |
| committer_name      | TEXT      |
| committer_email     | TEXT      |
| committer_when      | TIMESTAMP |
| commit_message      | TEXT      |
| tree_hash           | TEXT      |
| commit_parents      | JSON      |
+---------------------+-----------+
```

Commits contains all the [commits](https://git-scm.com/book/en/v2/Git-Internals-Git-Objects#_git_commit_objects) from all the references from all the repositories, not duplicated by repository. Note that you can have the same commit in several repositories. In that case, the commit will appears two times on the table, one per repository.

> Note that this table is not only showing `HEAD` commits but all the commits on the repository (that can be a lot more than the commits on `HEAD` reference).

### blobs
```sql
+---------------+-------+
| name          | type  |
+---------------+-------+
| repository_id | TEXT  |
| blob_hash     | TEXT  |
| blob_size     | INT64 |
| blob_content  | BLOB  |
+---------------+-------+
```

This table exposes blob objects, that are the content without path from files.

> Note that this table will return all the existing blobs on all the commits on all the repositories, potentially **a lot** of data. In most common cases you want to filter by commit, by reference or by repository.

### tree_entries
```sql
+-----------------+------+
| name            | type |
+-----------------+------+
| repository_id   | TEXT |
| tree_entry_name | TEXT |
| blob_hash       | TEXT |
| tree_hash       | TEXT |
| tree_entry_mode | TEXT |
+-----------------+------+
```

`tree_entries` table contains all the objects from all the repositories that are [tree objects](https://git-scm.com/book/en/v2/Git-Internals-Git-Objects#_git_commit_objects).


### files
```sql
+-----------------+-------+
| name            | type  |
+-----------------+-------+
| repository_id   | TEXT  |
| file_path       | TEXT  |
| blob_hash       | TEXT  |
| tree_hash       | TEXT  |
| tree_entry_mode | TEXT  |
| blob_content    | BLOB  |
| blob_size       | INT64 |
+-----------------+-------+
```

`files` is an utility table mixing `tree_entries` and `blobs` to create files. It includes the file path.

## Relation tables

### commit_blobs
```sql
+---------------+------+
| name          | type |
+---------------+------+
| repository_id | TEXT |
| commit_hash   | TEXT |
| blob_hash     | TEXT |
+---------------+------+
```

This table represents the relation between commits and blobs. With this table you can obtain all the blobs contained on a commit object.

### commit_trees
```sql
+---------------+------+
| name          | type |
+---------------+------+
| repository_id | TEXT |
| commit_hash   | TEXT |
| tree_hash     | TEXT |
+---------------+------+
```

This table represents the relation between commits and trees. With this table you can obtain all the tree entries contained on a commit object.

### ref_commits
```sql
+---------------+-------+
| name          | type  |
+---------------+-------+
| repository_id | TEXT  |
| commit_hash   | TEXT  |
| ref_name      | TEXT  |
| index         | INT64 |
+---------------+-------+
```

This table allow us to get the commit history from a specific reference name. `index` column represent the position of the commit from a specific reference.

This table it's like the [log](https://git-scm.com/docs/git-log) from a specific reference.

Commits will be repeated if they are in several repositories or references.

## Database diagram
<!--

repositories as r
-
repository_id string

remotes
-
repository_id string FK - r.repository_id
remote_name string
remote_push_url string
remote_fetch_url string
remote_push_refspec string
remote_fetch_refspec string

refs
-
repository_id string FK >- repositories.repository_id
ref_name string
commit_hash string FK >- commits.commit_hash

commits as c
-
repository_id string FK >- repositories.repository_id
commit_hash string
commit_author_name string
commit_author_email string
commit_author_when timestamp
committer_name string
committer_email string
committer_when timestamp
commit_message string
tree_hash string FK >- tree_entries.tree_hash
commit_parents array[string]

blobs as b
-
repository_id string FK >- repositories.repository_id
blob_hash string
blob_size number
blob_content blob

tree_entries as te
-
repository_id string FK >- repositories.repository_id
tree_hash string
blob_hash string FK >- blobs.blob_hash
tree_entry_mode number
tree_entry_name string

files as f
-
repository_id string FK >- repositories.repository_id
blob_hash string FK >- blobs.blob_hash
file_path string
tree_hash string FK >- tree_entries.tree_hash
tree_entry_mode number
blob_content blob
blob_size number

ref_commits
-
repository_id string FK >- repositories.repository_id
commit_hash string FK >- commits.commit_hash
ref_name string FK >- refs.ref_name
index number

commit_trees
-
repository_id string FK >- repositories.repository_id
commit_hash string FK >- commits.commit_hash
tree_hash string FK >- tree_entries.tree_hash

commit_blobs
-
repository_id string FK >- repositories.repository_id
commit_hash string FK >- commits.commit_hash
blob_hash string FK >- blobs.blob_hash

 -->

![gitbase schema](/docs/assets/gitbase-db-diagram.png)