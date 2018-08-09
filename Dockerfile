FROM golang:1.10 as builder

RUN mkdir -p /go/src/github.com/src-d/gitbase
WORKDIR /go/src/github.com/src-d/gitbase
COPY . .

RUN apt-get update && apt-get install libxml2-dev -y
RUN go get github.com/golang/dep/...
RUN dep ensure
RUN cd vendor/gopkg.in/bblfsh/client-go.v2 && make dependencies
RUN go install -v github.com/src-d/gitbase/...

FROM ubuntu:16.04

COPY --from=builder /go/bin/gitbase /bin
RUN mkdir -p /opt/repos

ENV GITBASE_USER=root
ENV GITBASE_PASSWORD=""
ENV GITBASE_REPOS=/opt/repos
EXPOSE 3306

ENV TINI_VERSION v0.17.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

RUN apt-get update \
    && apt-get install libxml2-dev git -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ENTRYPOINT ["/tini", "--"]

CMD gitbase server -v \
    --host=0.0.0.0 \
    --port=3306 \
    --user="$GITBASE_USER" \
    --password="$GITBASE_PASSWORD" \
    --directories="$GITBASE_REPOS"
