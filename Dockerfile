FROM golang:1.11-alpine as builder

RUN mkdir -p /go/src/github.com/src-d/gitbase
WORKDIR /go/src/github.com/src-d/gitbase
COPY . .

RUN apk add --update libxml2-dev git make bash gcc g++ curl oniguruma-dev
RUN go get github.com/golang/dep/...
RUN dep ensure
RUN cd vendor/gopkg.in/bblfsh/client-go.v2 && make dependencies
RUN make static-build

FROM alpine:3.8

COPY --from=builder /go/bin/gitbase /bin
RUN mkdir -p /opt/repos

ENV GITBASE_USER=root
ENV GITBASE_PASSWORD=""
ENV GITBASE_REPOS=/opt/repos
EXPOSE 3306

ENV TINI_VERSION v0.17.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-static /tini
RUN chmod +x /tini

ENTRYPOINT ["/tini", "--"]

CMD gitbase server -v \
    --host=0.0.0.0 \
    --port=3306 \
    --user="$GITBASE_USER" \
    --password="$GITBASE_PASSWORD" \
    --directories="$GITBASE_REPOS"
