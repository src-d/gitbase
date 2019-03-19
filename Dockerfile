#================================
# Stage 1: Build Gitbase
#================================
FROM golang:1.11-alpine as builder

ENV GITBASE_REPO=github.com/src-d/gitbase
ENV GITBASE_PATH=$GOPATH/src/$GITBASE_REPO

RUN apk add --no-cache git

COPY . $GITBASE_PATH
WORKDIR $GITBASE_PATH
RUN go build -ldflags="-X main.version=$(cat version.txt || echo "undefined") -X main.build=$(date +"%m-%d-%Y_%H_%M_%S") -X main.commit=$(git rev-parse --short HEAD) -s -w" -o /bin/gitbase ./cmd/gitbase

#=================================
# Stage 2: Start Gitbase Server
#=================================
FROM alpine:3.8

RUN apk add --no-cache mysql-client

RUN mkdir -p /opt/repos

EXPOSE 3306

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-static-amd64 /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

ENV GITBASE_USER=root
ENV GITBASE_PASSWORD=""
ENV GITBASE_REPOS=/opt/repos
ENV MYSQL_HOST=127.0.0.1

# copy build artifacts
COPY --from=builder /bin/gitbase /bin/gitbase
ADD init.sh ./init.sh
RUN chmod +x ./init.sh

ENTRYPOINT ["./init.sh"]
