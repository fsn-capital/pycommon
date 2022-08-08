SHELL := /bin/bash

.PHONY: all 
all: deps build

.PHONY: build
build:
	for dir in */; do \
		pushd $${dir} && poetry build && popd; \
	done
	

.PHONY: deps
deps:
	pip install poetry==1.1.14