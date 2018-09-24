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
| `GITBASE_LANGUAGE_CACHE_SIZE`           | size of the cache for the `language` UDF. The size is the maximum number of elements kept in the cache, 10000 by default |
| `GITBASE_UAST_CACHE_SIZE`           | size of the cache for the `uast` and `uast_mode` UDFs. The size is the maximum number of elements kept in the cache, 10000 by default |

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
  -h, --help             Show this help message

[server command options]
      -d, --directories= Path where the git repositories are located (standard and siva), multiple directories can be defined. Accepts globs.
          --depth=       load repositories looking at less than <depth> nested subdirectories. (default: 1000)
          --host=        Host where the server is going to listen (default: localhost)
      -p, --port=        Port where the server is going to listen (default: 3306)
      -u, --user=        User name used for connection (default: root)
      -P, --password=    Password used for connection
      -i, --index=       Directory where the gitbase indexes information will be persisted. (default: /var/lib/gitbase/index) [$GITBASE_INDEX_DIR]
          --no-squash    Disables the table squashing.
          --trace        Enables jaeger tracing [$GITBASE_TRACE]
      -r, --readonly     Only allow read queries. This disables creating and deleting indexes as well. [$GITBASE_READONLY]
          --no-git       disable the load of git standard repositories.
          --no-siva      disable the load of siva files.
      -v                 Activates the verbose mode

```
