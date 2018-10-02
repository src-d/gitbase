# Package configuration
PROJECT = gitbase
COMMANDS = cmd/gitbase

# Including ci Makefile
CI_REPOSITORY ?= https://github.com/src-d/ci.git
CI_PATH ?= $(shell pwd)/.ci
CI_VERSION ?= v1

UPGRADE_PRJ ?= "gopkg.in/src-d/go-mysql-server.v0"
UPGRADE_REV ?=  $(shell curl --silent "https://api.github.com/repos/src-d/go-mysql-server/commits/master" -H'Accept: application/vnd.github.VERSION.sha')

MAKEFILE := $(CI_PATH)/Makefile.main
$(MAKEFILE):
	git clone --quiet --branch $(CI_VERSION) --depth 1 $(CI_REPOSITORY) $(CI_PATH);

-include $(MAKEFILE)

# we still need to do this for windows
bblfsh-client:
	cd vendor/gopkg.in/bblfsh/client-go.v2 && make dependencies

dependencies: bblfsh-client

upgrade:
	go run tools/rev-upgrade/main.go -p $(UPGRADE_PRJ) -r $(UPGRADE_REV)

static-package:
	PACKAGE_NAME=gitbase_$(VERSION)_static_linux_amd64 ; \
	docker rm gitbase-temp ; \
	docker create --rm --name gitbase-temp $(DOCKER_ORG)/gitbase:$(VERSION) && \
	mkdir -p build/$${PACKAGE_NAME} && \
	docker cp gitbase-temp:/bin/gitbase build/$${PACKAGE_NAME} && \
	cd build && \
	tar czvf $${PACKAGE_NAME}.tar.gz $${PACKAGE_NAME} && \
	docker rm gitbase-temp

# target used in the Dockerfile to build the static binary
static-build: VERSION = $(shell git describe --exact-match --tags 2>/dev/null || dev-$(git rev-parse --short HEAD)$(test -n "`git status --porcelain`" && echo "-dirty" || true))
static-build: LD_FLAGS += -linkmode external -extldflags '-static -lz'
static-build: GO_BUILD_ARGS += -tags oniguruma
static-build:
	go install -v $(GO_BUILD_ARGS) github.com/src-d/gitbase/...
