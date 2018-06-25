# Configuration

## Environment variables

| Name                             | Description                                         |
|:---------------------------------|:----------------------------------------------------|
| `BBLFSH_ENDPOINT`                | bblfshd endpoint, default "127.0.0.1:9432"          |
| `GITBASE_BLOBS_MAX_SIZE`         | maximum blob size to return in MiB, default 5 MiB   |
| `GITBASE_BLOBS_ALLOW_BINARY`     | enable retrieval of binary blobs, default `false`   |
| `GITBASE_UNSTABLE_SQUASH_ENABLE` | enable join squash rule to improve query performance **experimental**. This optimization collects inner joins between tables with a set of supported conditions and converts them into a single node that retrieves the data in chained steps (getting first the commits and then the blobs of every commit instead of joining all commits and all blobs, for example).|
| `GITBASE_SKIP_GIT_ERRORS`        | do not stop queries on git errors, default disabled |

## Executable parameters