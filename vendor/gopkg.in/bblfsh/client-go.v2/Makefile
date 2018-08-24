# Package configuration
PROJECT = client-go
LIBUAST_VERSION ?= 2.0.1

TOOLS_FOLDER = tools

# Including ci Makefile
CI_REPOSITORY ?= https://github.com/src-d/ci.git
CI_PATH ?= .ci
CI_BRANCH ?= v1
MAKEFILE := $(CI_PATH)/Makefile.main
$(MAKEFILE):
	git clone --quiet --depth 1 -b $(CI_BRANCH) $(CI_REPOSITORY) $(CI_PATH)
-include $(MAKEFILE)

clean: clean-libuast
clean-libuast:
	find ./  -regex '.*\.[h,c]c?' ! -name 'bindings.h' -exec rm -f {} +

ifeq ($(OS),Windows_NT)
GOOS := windows
else
GOOS := $(shell uname -s | tr '[:upper:]' '[:lower:]')
endif

# 'Makefile::cgo-dependencies' target must be run before 'Makefile.main::dependencies' or 'go-get' will fail
dependencies: cgo-dependencies
	go get -v -t ./...
.PHONY: dependencies cgo-dependencies

ifeq ($(GOOS),windows)
cgo-dependencies:
	$(MAKE) clean-libuast && \
	cd $(TOOLS_FOLDER) && \
	curl -SLko archiver.exe https://github.com/mholt/archiver/releases/download/v2.0/archiver_windows_amd64.exe && \
	curl -SLko binaries.win64.mingw.zip https://github.com/bblfsh/libuast/releases/download/v$(LIBUAST_VERSION)/binaries.win64.mingw.zip && \
	./archiver.exe open binaries.win64.mingw.zip && \
	rm binaries.win64.mingw.zip && \
	rm archiver.exe && \
	echo done
else
ifeq ($(GOOS),darwin)
cgo-dependencies: unix-dependencies
else
cgo-dependencies: | check-gcc unix-dependencies
endif
endif
.PHONY: cgo-dependencies

check-gcc:
	@if \
		[[ -z `which gcc` ]] || \
		[[ -z `which g++` ]] || \
		[[ 5 -gt `gcc -dumpversion | sed 's/^[^0-9]*\([0-9][0-9]*\).*/\1/g'` ]] || \
		[[ 5 -gt `g++ -dumpversion | sed 's/^[^0-9]*\([0-9][0-9]*\).*/\1/g'` ]]; \
	then \
		echo -e "error; GCC and G++ v5 or greater are required \n"; \
		echo -e "- GCC: `gcc --version` \n"; \
		echo -e "- G++: `g++ --version` \n"; \
		exit 1; \
	fi;
.PHONY: check-gcc

unix-dependencies:
	curl -SL https://github.com/bblfsh/libuast/archive/v$(LIBUAST_VERSION).tar.gz | tar xz
	mv libuast-$(LIBUAST_VERSION)/src/* $(TOOLS_FOLDER)/.
	rm -rf libuast-$(LIBUAST_VERSION)
.PHONY: unix-dependencies
