# Package configuration
PROJECT = client-go
LIBUAST_VERSION ?= 1.9.1
GOPATH ?= $(shell go env GOPATH)

ifneq ($(OS),Windows_NT)
COPY = cp
else
COPY = copy
endif

# Including ci Makefile
MAKEFILE = Makefile.main
CI_REPOSITORY = https://github.com/src-d/ci.git
CI_FOLDER = .ci

TOOLS_FOLDER = tools

$(MAKEFILE):
	@(git clone --quiet $(CI_REPOSITORY) $(CI_FOLDER) && \
	$(COPY) $(CI_FOLDER)/$(MAKEFILE) .);

-include $(MAKEFILE)
GOGET ?= go get

clean: clean-libuast
clean-libuast:
	find ./  -regex '.*\.[h,c]c?' ! -name 'bindings.h' -exec rm -f {} +

dependencies: cgo-dependencies
ifneq ($(OS),Windows_NT)
cgo-dependencies:
	curl -SL https://github.com/bblfsh/libuast/releases/download/v$(LIBUAST_VERSION)/libuast-v$(LIBUAST_VERSION).tar.gz | tar xz
	mv libuast-v$(LIBUAST_VERSION)/src/* $(TOOLS_FOLDER)/.
	rm -rf libuast-v$(LIBUAST_VERSION)
	$(GOGET) ./...
else
binaries.win64.mingw\lib:
	go get -v github.com/mholt/archiver/cmd/archiver
	cd $(TOOLS_FOLDER) && \
	curl -SLko binaries.win64.mingw.zip https://github.com/bblfsh/libuast/releases/download/v$(LIBUAST_VERSION)/binaries.win64.mingw.zip && \
	$(GOPATH)\bin\archiver open binaries.win64.mingw.zip && \
	del /q binaries.win64.mingw.zip && echo done

cgo-dependencies: binaries.win64.mingw\lib
	$(GOGET) ./...
endif  # !Windows_NT

# $(DEPENDENCIES) it's allowed to file since the code is not compilable
# without libuast.
.IGNORE: $(DEPENDENCIES)
