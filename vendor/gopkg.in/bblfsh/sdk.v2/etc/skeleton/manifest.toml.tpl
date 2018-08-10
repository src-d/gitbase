# Manifest contains metadata about the driver. To learn about the different
# values in this manifest refer to:
#   https://github.com/bblfsh/sdk/blob/master/manifest/manifest.go

language = "{{.Language}}"

# status describes the current development status of the driver, the valid
# values for status are: `planning`, `pre-alpha`, `alpha`, `beta`, `stable`,
# `mature` or `inactive`.
status = "planning"

# documentation block is use to render the README.md file.
[documentation]
description  = """
{{.Language}} driver for [babelfish](https://github.com/bblfsh/server).
"""

[runtime]
# os defines in with distribution the runtime is executed (and the build
# system). The valid values are `alpine` and `debian`. Alpine is preferred
# since is a micro-distribution, but sometimes is hard or impossible to use
# due to be based on musl and not it libc.
os = "{{.OS}}"

# go_version describes the version being use to build the driver Go code.
go_version = "1.9"

# native_version describes the version or versions being use to build and
# execute the native code, you should define at least one. (eg.: "1.8").
native_version = []
