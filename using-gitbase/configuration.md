# Configuration

## Environment variables

| Name | Description |
| :--- | :--- |
| `BBLFSH_ENDPOINT` | bblfshd endpoint, default "127.0.0.1:9432" |
| `PILOSA_ENDPOINT` | pilosa endpoint, default "[http://localhost:10101](http://localhost:10101)" |
| `GITBASE_BLOBS_MAX_SIZE` | maximum blob size to return in MiB, default 5 MiB |
| `GITBASE_BLOBS_ALLOW_BINARY` | enable retrieval of binary blobs, default `false` |
| `GITBASE_SKIP_GIT_ERRORS` | do not stop queries on git errors, default disabled |
| `GITBASE_INDEX_DIR` | directory to save indexes, default `/var/lib/gitbase/index` |
| `GITBASE_TRACE` | enable jaeger tracing, default disabled |
| `GITBASE_READONLY` | allow read queries only, disabling creating and deleting indexes, default disabled |

### Jaeger tracing variables

_Extracted from_ [https://github.com/jaegertracing/jaeger-client-go/blob/master/README.md](https://github.com/jaegertracing/jaeger-client-go/blob/master/README.md)

| Property | Description |
| :--- | :--- |
| JAEGER\_SERVICE\_NAME | The service name |
| JAEGER\_AGENT\_HOST | The hostname for communicating with agent via UDP |
| JAEGER\_AGENT\_PORT | The port for communicating with agent via UDP |
| JAEGER\_REPORTER\_LOG\_SPANS | Whether the reporter should also log the spans |
| JAEGER\_REPORTER\_MAX\_QUEUE\_SIZE | The reporter's maximum queue size |
| JAEGER\_REPORTER\_FLUSH\_INTERVAL | The reporter's flush interval \(ms\) |
| JAEGER\_SAMPLER\_TYPE | The sampler type |
| JAEGER\_SAMPLER\_PARAM | The sampler parameter \(number\) |
| JAEGER\_SAMPLER\_MANAGER\_HOST\_PORT | The host name and port when using the remote controlled sampler |
| JAEGER\_SAMPLER\_MAX\_OPERATIONS | The maximum number of operations that the sampler will keep track of |
| JAEGER\_SAMPLER\_REFRESH\_INTERVAL | How often the remotely controlled sampler will poll jaeger-agent for the appropriate sampling strategy |
| JAEGER\_TAGS | A comma separated list of `name = value` tracer level tags, which get added to all reported spans. The value can also refer to an environment variable using the format `${envVarName:default}`, where the `:default` is optional, and identifies a value to be used if the environment variable cannot be found |
| JAEGER\_DISABLED | Whether the tracer is disabled or not. If true, the default `opentracing.NoopTracer` is used. |
| JAEGER\_RPC\_METRICS | Whether to store RPC metrics |

## Command line arguments

```text
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

```text
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
          --pilosa=      URL to your pilosa server (default: http://localhost:10101) [$PILOSA_ENDPOINT]
      -i, --index=       Directory where the gitbase indexes information will be persisted. (default: /var/lib/gitbase/index) [$GITBASE_INDEX_DIR]
          --no-squash    Disables the table squashing.
          --trace        Enables jaeger tracing [$GITBASE_TRACE]
      -r, --readonly     Only allow read queries. This disables creating and deleting indexes as well. [$GITBASE_READONLY]
          --no-git       disable the load of git standard repositories.
          --no-siva      disable the load of siva files.
      -v                 Activates the verbose mode
```

