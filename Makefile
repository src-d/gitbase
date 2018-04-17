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

dependencies:
	cd vendor/gopkg.in/bblfsh/client-go.v2 && make dependencies

bblfsh:
	docker run -d --rm --name bblfshd --privileged -p 9432:9432 -v /var/lib/bblfshd:/var/lib/bblfshd bblfsh/bblfshd:v2.4.2

bblfsh-drivers:
	docker exec -it bblfshd bblfshctl driver install --recommended

bblfsh-list-drivers:
	docker exec -it bblfshd bblfshctl driver list
