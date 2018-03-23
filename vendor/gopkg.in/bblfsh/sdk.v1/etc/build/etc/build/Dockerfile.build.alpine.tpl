FROM ${DOCKER_BUILD_NATIVE_IMAGE}

# remove any pre-installed Go SDK in the base image and reset GOROOT
RUN sh -c '[[ ! -z $(which go) ]] && rm -rf $(go env GOROOT) || true'
ENV GOROOT=""

ENV GOLANG_SRC_URL https://golang.org/dl/go${RUNTIME_GO_VERSION}.src.tar.gz

# from https://github.com/docker-library/golang/blob/master/1.9/alpine3.7/Dockerfile

RUN apk add --update --no-cache ca-certificates openssl && update-ca-certificates
RUN wget https://raw.githubusercontent.com/docker-library/golang/e63ba9c5efb040b35b71e16722b71b2931f29eb8/${RUNTIME_GO_VERSION}/alpine3.7/no-pic.patch -O /no-pic.patch -O /no-pic.patch

RUN set -ex \
	&& apk add --no-cache --virtual .build-deps \
		bash \
		gcc \
		musl-dev \
		openssl \
		go \
	\
	&& export GOROOT_BOOTSTRAP="$(go env GOROOT)" \
	\
	&& wget -q "$GOLANG_SRC_URL" -O golang.tar.gz \
	&& tar -C /usr/local -xzf golang.tar.gz \
	&& rm golang.tar.gz \
	&& cd /usr/local/go/src \
	&& patch -p2 -i /no-pic.patch \
	&& ./make.bash \
	\
	&& rm -rf /*.patch \
	&& apk del .build-deps

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"
