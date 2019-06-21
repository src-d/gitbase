# Configuration

## Environment variables

| Name                         | Description                                                                        |
|:-----------------------------|:-----------------------------------------------------------------------------------|
| `BBLFSH_ENDPOINT`            | bblfshd endpoint, default "127.0.0.1:9432"                                         |
| `GITBASE_BLOBS_MAX_SIZE`     | maximum blob size to return in MiB, default 5 MiB                                  |
| `GITBASE_BLOBS_ALLOW_BINARY` | enable retrieval of binary blobs, default `false`                                  |
| `GITBASE_SKIP_GIT_ERRORS`    | do not stop queries on git errors, default disabled                                |
| `GITBASE_INDEX_DIR`          | directory to save indexes, default `/var/lib/gitbase/index`                        |
| `GITBASE_TRACE`              | enable jaeger tracing, default disabled                                            |
| `GITBASE_READONLY`           | allow read queries only, disabling creating and deleting indexes, default disabled |
| `GITBASE_LANGUAGE_CACHE_SIZE`| size of the cache for the `language` UDF. The size is the maximum number of elements kept in the cache, 10000 by default |
| `GITBASE_UAST_CACHE_SIZE`    | size of the cache for the `uast` and `uast_mode` UDFs. The size is the maximum number of elements kept in the cache, 10000 by default |
| `GITBASE_CACHESIZE_MB`       | size of the cache for git objects specified as MB                                  |
| `GITBASE_CONNECTION_TIMEOUT` | timeout in seconds used for client connections on write and reads. No timeout by default.     |
| `GITBASE_USER_FILE`          | JSON file with user credentials                                                    |
| `GITBASE_MAX_UAST_BLOB_SIZE`          | Max size of blobs to send to be parsed by bblfsh. Default: 5242880 (5MB)                                                    |
| `GITBASE_LOG_LEVEL`          | minimum logging level to show, use `fatal` to suppress most messages. Default: `info` |

## Configuration from `go-mysql-server`

<!-- BEGIN CONFIG -->
| Name | Type | Description |
|:-----|:-----|:------------|
|`INMEMORY_JOINS`|environment|If set it will perform all joins in memory. Default is off.|
|`inmemory_joins`|session|If set it will perform all joins in memory. Default is off. This has precedence over `INMEMORY_JOINS`.|
|`MAX_MEMORY_JOIN`|environment|The maximum number of memory, in megabytes, that can be consumed by go-mysql-server before switching to multipass mode in joins. Default is the 20% of all available physical memory.|
|`max_memory_joins`|session|The maximum number of memory, in megabytes, that can be consumed by go-mysql-server before switching to multipass mode in joins. Default is the 20% of all available physical memory. This has precedence over `MAX_MEMORY_JOIN`.|
|`DEBUG_ANALYZER`|environment|If set, the analyzer will print debug messages. Default is off.|
|`PILOSA_INDEX_THREADS`|environment|Number of threads used in index creation. Default is the number of cores available in the machine.|
|`pilosa_index_threads`|environment|Number of threads used in index creation. Default is the number of cores available in the machine. This has precedence over `PILOSA_INDEX_THREADS`.|
<!-- END CONFIG -->

### Jaeger tracing variables

*Extracted from https://github.com/jaegertracing/jaeger-client-go/blob/master/README.md*

Property| Description
--- | ---
JAEGER_SERVICE_NAME | The service name
JAEGER_AGENT_HOST | The hostname for communicating with agent via UDP
JAEGER_AGENT_PORT | The port for communicating with agent via UDP
JAEGER_REPORTER_LOG_SPANS | Whether the reporter should also log the spans
JAEGER_REPORTER_MAX_QUEUE_SIZE | The reporter's maximum queue size
JAEGER_REPORTER_FLUSH_INTERVAL | The reporter's flush interval (ms)
JAEGER_SAMPLER_TYPE | The sampler type
JAEGER_SAMPLER_PARAM | The sampler parameter (number)
JAEGER_SAMPLER_MANAGER_HOST_PORT | The host name and port when using the remote controlled sampler
JAEGER_SAMPLER_MAX_OPERATIONS | The maximum number of operations that the sampler will keep track of
JAEGER_SAMPLER_REFRESH_INTERVAL | How often the remotely controlled sampler will poll jaeger-agent for the appropriate sampling strategy
JAEGER_TAGS | A comma separated list of `name = value` tracer level tags, which get added to all reported spans. The value can also refer to an environment variable using the format `${envVarName:default}`, where the `:default` is optional, and identifies a value to be used if the environment variable cannot be found
JAEGER_DISABLED | Whether the tracer is disabled or not. If true, the default `opentracing.NoopTracer` is used.
JAEGER_RPC_METRICS | Whether to store RPC metrics

## Command line arguments

```
Please specify one command of: server or version
Usage:
  gitbase [OPTIONS] <server | version>

Help Options:
  -h, --help  Show this help message

Available commands:
  server   Starts a gitbase server instance
  version  Show the version information
```

`server` command contains the following options:

```
Usage:
  gitbase [OPTIONS] server [server-OPTIONS]

Starts a gitbase server instance

By default when gitbase encounters an error in a repository it
stops the query. With GITBASE_SKIP_GIT_ERRORS variable it won't
complain and just skip those rows or repositories.

Help Options:
  -h, --help                                           Show this help message

[server command options]
          --db=                                        Database name (default: gitbase)
      -d, --directories=                               Path where standard git repositories are located,
                                                       multiple directories can be defined.
          --format=[git|siva]                          Library format (default: git)
          --bucket=                                    Bucketing level to use with siva libraries (default: 2)
          --bare                                       Sets the library to use bare git repositories, used
                                                       only with git format libraries
          --non-bare                                   Sets the library to use non bare git repositories,
                                                       used only with git format libraries
          --non-rooted                                 Disables treating siva files as rooted repositories
          --host=                                      Host where the server is going to listen (default:
                                                       localhost)
      -p, --port=                                      Port where the server is going to listen (default:
                                                       3306)
      -u, --user=                                      User name used for connection (default: root)
      -P, --password=                                  Password used for connection
      -U, --user-file=                                 JSON file with credentials list [$GITBASE_USER_FILE]
      -t, --timeout=                                   Timeout in seconds used for connections
                                                       [$GITBASE_CONNECTION_TIMEOUT]
      -i, --index=                                     Directory where the gitbase indexes information will
                                                       be persisted. (default: /var/lib/gitbase/index)
                                                       [$GITBASE_INDEX_DIR]
          --cache=                                     Object cache size in megabytes (default: 512)
                                                       [$GITBASE_CACHESIZE_MB]
          --parallelism=                               Maximum number of parallel threads per table. By
                                                       default, it's the number of CPU cores. 0 means
                                                       default, 1 means disabled.
          --no-squash                                  Disables the table squashing.
          --trace                                      Enables jaeger tracing [$GITBASE_TRACE]
      -r, --readonly                                   Only allow read queries. This disables creating and
                                                       deleting indexes as well. Cannot be used with
                                                       --user-file. [$GITBASE_READONLY]
      -v                                               Activates the verbose mode
          --log-level=[info|debug|warning|error|fatal] logging level (default: info) [$GITBASE_LOG_LEVEL]
```