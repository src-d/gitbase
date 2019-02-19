# Security

## User credentials

User credentials can be specified in the command line or in the user file. For a single user this can be done with parameters `--user` and `--password`:

```
gitbase server --user root --password r00tp4ssword! -d /my/repositories/path
```

If you want to have more than one user or not having the password in plain text, then the user file can be useful. You can keep passwords, permissions and user names in the following format:

```json
[
  {
    "name": "root",
    "password": "*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19",
    "permissions": ["read", "write"]
  },
  {
    "name": "user",
    "password": "plain_passw0rd!"
  }
]
```

You can either specify passwords as a plain text or hashed. Hashed version uses the same format as MySQL 5.x passwords. You can generate the native password with this command, remember to prefix the hash with `*`:

```
echo -n password | openssl sha1 -binary | openssl sha1 | tr '[:lower:]' '[:upper:]'
```

There are two permissions you can set for users, `read` and `write`. `read` only allows executing read-only queries that do not modify the internal state or the data itself. `write` is needed to create and delete indexes or lock tables. If no permissions are set for a user the default permission is `read`.

Then you can specify which user file to use with the `--user-file` parameter:

```
gitbase server --user-file /path/to/user-file.json -d /my/repositories/path
```

## Audit

Gitbase offers audit trails on logs. Right now, we have three different kinds of records: `authentication`, `authorization` and `query`

### Authentication

Record triggered when a user is trying to connect to gitbase. It contains the following information:

- action: Always `authentication`.
- system: Always `audit`
- address: Address of the client trying to connect.
- err: Human readable error if the authentication was not successful.
- success: True or false depending on whether the client authenticated correctly or not.
- user: Username trying to connect

Example:

```
action=authentication address="127.0.0.1:41720" err="Access denied for user 'test' (errno 1045) (sqlstate 28000)" success=false system=audit user=test
```

### Authorization

Record triggered checking when a user is authorized to execute a specific valid query with their permissions. It contains the following information:

- action: Always `authorization`.
- system: Always `audit`
- address: Address of the client.
- success: True or false depending on whether the client has been authorized correctly or not.
- user: Username trying to execute the query.
- connection_id: Unique connection identifier of the current request.
- permission: Permission needed to execute the query.
- pid: Pid returns the process ID associated with this context. It will change in subsequent queries sent using the same connection.
- query: Query that client is trying to execute.

Example:

```
INFO[0007] audit trail                                   action=authorization address="127.0.0.1:41610" connection_id=1 permission=read pid=1 query="select @@version_comment limit 1" success=true system=audit user=root
```

### Query

Record triggered at the end of the executed query. It contains the following information:

- action: Always `query`.
- system: Always `audit`
- address: Address of the client.
- success: True or false depending on whether the query was executed or not.
- user: Username executing the query.
- connection_id: Unique connection identifier of the current request.
- pid: Pid returns the process ID associated with this context. It will change in subsequent queries sent using the same connection.
- query: Query that client is trying to execute.
- err: If `success=false`. Human readable error describing the problem.
- duration: Time that the query took to execute. If the format of the logs is `JSON` this duration unit is nanoseconds.

Examples:

```
INFO[0983] audit trail                                   action=query address="127.0.0.1:42428" connection_id=2 duration=22.707457818s pid=6 query="select count(*) from commits" success=true system=audit user=root
```

```
INFO[0910] audit trail                                   action=query address="127.0.0.1:42428" connection_id=2 duration="77.822µs" err="syntax error at position 6 near 'wrong'" pid=5 query="wrong query" success=false system=audit user=root
```