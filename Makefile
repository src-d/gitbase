# Package configuration
PROJECT = gitbase
COMMANDS = cmd/gitbase

# Including ci Makefile
CI_REPOSITORY ?= https://github.com/src-d/ci.git
CI_PATH ?= $(shell pwd)/.ci
CI_VERSION ?= v1

MAKEFILE := $(CI_PATH)/Makefile.main
$(MAKEFILE):
	git clone --quiet --branch $(CI_VERSION) --depth 1 $(CI_REPOSITORY) $(CI_PATH);

-include $(MAKEFILE)

bblfsh-client:
	cd vendor/gopkg.in/bblfsh/client-go.v2 && make dependencies

dependencies: bblfsh-client
