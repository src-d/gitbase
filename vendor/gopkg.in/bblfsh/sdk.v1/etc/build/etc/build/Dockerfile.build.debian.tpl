FROM ${DOCKER_BUILD_NATIVE_IMAGE}

# remove any pre-installed Go SDK in the base image and reset GOROOT
RUN sh -c '[[ ! -z $(which go) ]] && rm -rf $(go env GOROOT) || true'
ENV GOROOT=""

ENV GOLANG_DOWNLOAD_URL https://golang.org/dl/go${RUNTIME_GO_VERSION}.linux-amd64.tar.gz

RUN curl -fsSL "$GOLANG_DOWNLOAD_URL" -o golang.tar.gz \
	&& tar -C /usr/local -xzf golang.tar.gz \
	&& rm golang.tar.gz

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"