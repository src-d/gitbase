# variables being included from the `manifest.mk`
LANGUAGE ?=
RUNTIME_OS ?=
RUNTIME_NATIVE_VERSION ?=
RUNTIME_GO_VERSION ?=

# get the git commit
GIT_COMMIT=$(shell git rev-parse HEAD | cut -c1-7)
GIT_DIRTY=$(shell test -n "`git status --porcelain`" && echo "-dirty" || true)

# optional variables
DRIVER_DEV_PREFIX := dev
DRIVER_VERSION ?= $(DRIVER_DEV_PREFIX)-$(GIT_COMMIT)$(GIT_DIRTY)

DOCKER_IMAGE ?= bblfsh/$(LANGUAGE)-driver
DOCKER_IMAGE_VERSIONED ?= $(call escape_docker_tag,$(DOCKER_IMAGE):$(DRIVER_VERSION))
DOCKER_BUILD_NATIVE_IMAGE ?= $(DOCKER_IMAGE)-build
DOCKER_BUILD_DRIVER_IMAGE ?= $(DOCKER_IMAGE)-build-with-go

# defined behaviour for builds inside travis-ci
ifneq ($(origin CI), undefined)
    # if we are inside CI, verbose is enabled by default
	VERBOSE := true
endif

# if TRAVIS_TAG defined DRIVER_VERSION is overrided
ifneq ($(TRAVIS_TAG), )
    DRIVER_VERSION := $(TRAVIS_TAG)
endif

# if we are not in master, and it's not a tag the push is disabled
ifneq ($(TRAVIS_BRANCH), master)
	ifeq ($(TRAVIS_TAG), )
        pushdisabled = "push disabled for non-master branches"
	endif
endif

# if this is a pull request, the push is disabled
ifneq ($(TRAVIS_PULL_REQUEST), false)
        pushdisabled = "push disabled for pull-requests"
endif
