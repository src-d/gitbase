-include .sdk/Makefile

$(if $(filter true,$(sdkloaded)),,$(error You must install bblfsh-sdk with: "bblfsh-sdk prepare-build"))

test-native-internal:
	cd native; \
	echo "not implemented"

build-native-internal:
	cd native; \
	echo "not implemented"
	echo -e "#!/bin/bash\necho 'not implemented'" > $(BUILD_PATH)/bin/native
	chmod +x $(BUILD_PATH)/bin/native
