define helptext
bblfsh-sdk build system
=======================

The bblfsh build system helps to you to build and test a bblfsh driver. It
contains several rules to build docker containers, execute tests, validate
the driver, etc.

RULES
make build            builds driver's docker image, compiling the normalizer
                      component and the native component if needed calling the
                      rules: build-native and build-driver. Builds the required
                      docker images to do this.

make build-native     compiles the native component if needed, in interpreted
                      languages only prepares the scripts to execute the
                      component. To perform this is executes make calling the
                      private rule: `build-native-internal` defined in the
                      Makefile in the root of the project inside of the build
                      container.

make build-driver     compiles the normalizer component.

make test             execute all the unit tests of the components inside of the
                      build containers. It build the docker images if need it.

make test-native      execute the unit test for the native component. To perform
                      this is execute make calling the private rule:
                      `test-native-internal` defined in the Makefile in the root
                      of the project inside of the build container.

make test-driver      execute the unit test for the normalizer component.

make push             push the driver's docker image to the Docker registry. This
                      rule can be only executed inside of a Travis-CI environment
                      and just when is running for a tag.

make integration-test execute integration tests.

make validate         validates the current driver.

make clean            cleans all the build directories.

INTERNAL RULES
Two internal rules are required to run the test and the build main rules:
`test-native-internal` and `build-native-internal`. This rules are defined in the
Makefile in the root of the driver, they contain the language specific commands
for the native runtime.

To further documentation please go to https://doc.bblf.sh
endef

help:
	@echo "$$helptext" | more
