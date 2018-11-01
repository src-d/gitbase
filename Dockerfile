FROM alpine:3.8

COPY build/bin/gitbase /bin
RUN mkdir -p /opt/repos

ENV GITBASE_USER=root
ENV GITBASE_PASSWORD=""
ENV GITBASE_REPOS=/opt/repos
EXPOSE 3306

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-static-amd64 /tini
RUN chmod +x /tini

RUN echo 'http://dl-cdn.alpinelinux.org/alpine/v3.1/main' >> /etc/apk/repositories && \ 
	apk --no-cache add 'libxml2==2.9.2-r2' git oniguruma libc6-compat &&  \
	apk clean

ENTRYPOINT ["/tini", "--"]

CMD gitbase server -v \
    --host=0.0.0.0 \
    --port=3306 \
    --user="$GITBASE_USER" \
    --password="$GITBASE_PASSWORD" \
    --directories="$GITBASE_REPOS"
