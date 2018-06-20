# Package configuration
PROJECT = client-go
LIBUAST_VERSION ?= 1.9.1
GOPATH ?= $(shell go env GOPATH)

TOOLS_FOLDER = tools

ifneq ($(OS),Windows_NT)
UNAME := $(shell uname)
COPY = cp
else
UNAME := Windows_NT
COPY = copy
endif

# 'Makefile::cgo-dependencies' target must be run before 'Makefile.main::dependencies' or 'go-get' will fail
dependencies: cgo-dependencies

# Including ci Makefile
CI_REPOSITORY ?= https://github.com/src-d/ci.git
CI_PATH ?= $(shell pwd)/.ci
MAKEFILE := $(CI_PATH)/Makefile.main
$(MAKEFILE):
	git clone --quiet --depth 1 $(CI_REPOSITORY) $(CI_PATH);
-include $(MAKEFILE)

clean: clean-libuast
clean-libuast:
	find ./  -regex '.*\.[h,c]c?' ! -name 'bindings.h' -exec rm -f {} +

ifeq ($(OS),Windows_NT)
cgo-dependencies:
	go get -v github.com/mholt/archiver/cmd/archiver
	cd $(TOOLS_FOLDER) && \
	curl -SLko binaries.win64.mingw.zip https://github.com/bblfsh/libuast/releases/download/v$(LIBUAST_VERSION)/binaries.win64.mingw.zip && \
	$(GOPATH)\bin\archiver open binaries.win64.mingw.zip && \
	del /q binaries.win64.mingw.zip && echo done
else
ifeq ($(UNAME),Darwin)
cgo-dependencies: unix-dependencies
else
cgo-dependencies: | check-gcc unix-dependencies
check-gcc:
	@if \
		[[ -z `which gcc` ]] || \
		[[ -z `which g++` ]] || \
		[[ 5 -gt `gcc -dumpversion | sed -r 's/^[^0-9]*([0-9]+).*/\1/g'` ]] || \
		[[ 5 -gt `g++ -dumpversion | sed -r 's/^[^0-9]*([0-9]+).*/\1/g'` ]]; \
	then \
		echo -e "error; GCC and G++ v5 or greater are required \n"; \
		echo -e "- GCC: `gcc --version` \n"; \
		echo -e "- G++: `g++ --version` \n"; \
		exit 1; \
	fi;
endif
endif

unix-dependencies:
	curl -SL https://github.com/bblfsh/libuast/releases/download/v$(LIBUAST_VERSION)/libuast-v$(LIBUAST_VERSION).tar.gz | tar xz
	mv libuast-v$(LIBUAST_VERSION)/src/* $(TOOLS_FOLDER)/.
	rm -rf libuast-v$(LIBUAST_VERSION)
