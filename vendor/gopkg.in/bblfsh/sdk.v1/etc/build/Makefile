# This is the entrypoint of the bblfsh make SDK, the SDK should be stored at
# `sdklocation`, generated executing `bblfsh-sdk prepare-build`. Many different
# vars as `LANGUAGE` are required, this variables are extracting from the
# `manifest.toml` at the root and the project translating it to a make include
# at `manifest` using `bblfsh-sdk manifest` tool.

location = $(shell pwd)
sdklocation = $(location)/.sdk
manifest = $(sdklocation)/make/manifest.mk
tmplocation = $(sdklocation)/tmp
$(shell mkdir -p $(tmplocation))

bblfsh-sdk := $(shell command -v bblfsh-sdk 2> /dev/null)
bblfsh-sdk-tools := $(shell command -v bblfsh-sdk-tools 2> /dev/null)
in-container := $(shell echo $$ENVIRONMENT)
host-platform := $(shell echo $$HOST_PLATFORM)

ifdef bblfsh-sdk-tools # run only with Golang
    ifdef in-container
        bblfsh-sdk-tools :=  go run /go/src/gopkg.in/bblfsh/sdk.v1/cli/bblfsh-sdk-tools/main.go
    endif

    $(shell $(bblfsh-sdk-tools) envvars > $(manifest))
endif

include $(sdklocation)/make/manifest.mk
include $(sdklocation)/make/functions.mk
include $(sdklocation)/make/bootstrap.mk
include $(sdklocation)/make/help.mk
include $(sdklocation)/make/rules.mk
sdkloaded = true
