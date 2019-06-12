# Package configuration
PROJECT = gitbase
COMMANDS = cmd/gitbase

# Including ci Makefile
CI_REPOSITORY ?= https://github.com/src-d/ci.git
CI_PATH ?= $(shell pwd)/.ci
CI_VERSION ?= v1

UPGRADE_PRJ ?= "github.com/src-d/go-mysql-server"
UPGRADE_REV ?=  $(shell curl --silent "https://api.github.com/repos/src-d/go-mysql-server/commits/master" -H'Accept: application/vnd.github.VERSION.sha')

MAKEFILE := $(CI_PATH)/Makefile.main
$(MAKEFILE):
	git clone --quiet --branch $(CI_VERSION) --depth 1 $(CI_REPOSITORY) $(CI_PATH);

-include $(MAKEFILE)


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
static-build: VERSION = $(shell git describe --exact-match --tags 2>/dev/null || "dev-$(git rev-parse --short HEAD)$(test -n "`git status --porcelain`" && echo "-dirty" || true)")
static-build: LD_FLAGS += -linkmode external -extldflags '-static -lz' -s -w
static-build: GO_BUILD_PATH ?= github.com/src-d/gitbase/...
static-build:
	go build $(GO_BUILD_ARGS) -ldflags="$(LD_FLAGS)" -v  $(GO_BUILD_PATH)

ci-e2e: packages
	go test ./e2e -gitbase-version="$(TRAVIS_TAG)" \
	-must-run \
	-gitbase-bin="$(TRAVIS_BUILD_DIR)/build/gitbase_linux_amd64/gitbase" \
	-gitbase-repos="$(TRAVIS_BUILD_DIR)/.." -v
