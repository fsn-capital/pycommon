.PHONY: all 
all: deps build-gcs build-utils

.PHONY: build-gcs
build-gcs:
	pushd gcs && poetry build && popd

.PHONY: build-utils 
build-utils:
	pushd utils && poetry build && popd 

.PHONY: deps
deps:
	pip install poetry==1.1.14