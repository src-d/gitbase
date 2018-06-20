# Package configuration
PROJECT = gitbase
COMMANDS = cmd/gitbase

# Including ci Makefile
CI_REPOSITORY ?= https://github.com/src-d/ci.git
CI_PATH ?= .ci
CI_VERSION ?= v1

MAKEFILE := $(CI_PATH)/Makefile.main
$(MAKEFILE):
	git clone --quiet --branch $(CI_VERSION) --depth 1 $(CI_REPOSITORY) $(CI_PATH);

# Run tests with external services only on Travis/Linux
GO_TEST_FLAGS ?= -v -short
ifeq ($(TRAVIS),true)
ifeq ($(TRAVIS_OS),linux)
GO_TEST_FLAGS = -v
endif
endif

-include $(MAKEFILE)

ifeq ($(TRAVIS),true)
ifeq ($(TRAVIS_OS),linux)
prepare-services:
	docker pull pilosa/pilosa:v0.9.0 && \
	docker run -d --name pilosa -p 127.0.0.1:10101:10101 pilosa/pilosa:v0.9.0 && \
        docker run -d --name bblfshd --privileged -p 9432:9432 -v /var/lib/bblfshd:/var/lib/bblfshd bblfsh/bblfshd && \
        docker exec -it bblfshd bblfshctl driver install python bblfsh/python-driver && \
        docker exec -it bblfshd bblfshctl driver install php bblfsh/php-driver

bblfsh-client: install-gcc-6

install-gcc-6:
	export DEBIAN_FRONTEND=noninteractive && \
	sudo -E apt-add-repository -y "ppa:ubuntu-toolchain-r/test" && \
	sudo -E apt-get -yq update && \
	sudo -E apt-get -yq --no-install-suggests --no-install-recommends --force-yes install gcc-6 g++-6 && \
	sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-6 90 && \
	sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-6 90
endif
endif

bblfsh-client:
	cd vendor/gopkg.in/bblfsh/client-go.v2 && make dependencies

dependencies: bblfsh-client
