JNAERATOR_VERSION=ac73c9e
JARS_DIR=./lib
JAR=$(JARS_DIR)/enry.jar
JNAERATOR_DIR=./.jnaerator
JNAERATOR_JAR=$(JNAERATOR_DIR)/jnaerator.jar
JNAERATOR_JAR_URL="https://jitpack.io/com/github/nativelibs4java/JNAerator/jnaerator/$(JNAERATOR_VERSION)/jnaerator-$(JNAERATOR_VERSION)-shaded.jar"
RESOURCES_SRC=../.shared
RESOURCES_DIR=./shared
HEADER_FILE=$(RESOURCES_DIR)/libenry.h

SHELL := /bin/bash

all: $(JAR)

$(JAR): $(RESOURCES_DIR) $(JNAERATOR_JAR)
	mkdir -p lib && \
	java -jar $(JNAERATOR_JAR) \
		-package tech.sourced.enry.nativelib \
		-library enry \
		$(HEADER_FILE) \
		-o $(JARS_DIR) \
		-mode StandaloneJar \
		-runtime JNA;

$(RESOURCES_DIR): os-shared-lib
	cp -R $(RESOURCES_SRC) $(RESOURCES_DIR)

$(JNAERATOR_JAR): $(RESOURCES_DIR)
	mkdir $(JNAERATOR_DIR) && \
	wget $(JNAERATOR_JAR_URL) -O $(JNAERATOR_JAR)

os-shared-lib:
	@os_name="$(shell uname -s)" && \
	if [ "$$os_name" == "Linux" ]; then \
		$(MAKE) linux-shared; \
	elif [ "$$os_name" == "Darwin" ]; then \
		$(MAKE) darwin-shared; \
	else \
		echo "Unsupported operating system, can't build shared library"; \
		exit 1; \
	fi;

linux-shared:
	cd .. && \
	$(MAKE) linux-shared

darwin-shared:
	cd .. && \
	$(MAKE) darwin-shared

test:
	./sbt clean test

package:
	./sbt clean assembly

clean:
	rm -rf $(JAR)
	rm -rf $(RESOURCES_DIR)

