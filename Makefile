SHELL := /bin/bash
POETRY_VERSION := 1.1.14
MYPY_VERSION := 0.971

.PHONY: all 
all: deps build

.PHONY: ci
ci: deps type-checker

.PHONY: build
build:
	for dir in */; do \
		pushd $${dir} && poetry build && popd || exit 1; \
	done
	
.PHONY: type-checker
type-checker:
	for dir in */; do \
		pushd $${dir} && poetry install && source $$(poetry env info --path)/bin/activate && mypy . && deactivate && popd || exit 1; \
	done

.PHONY: deps
deps:
	pip install poetry==${POETRY_VERSION} mypy==${MYPY_VERSION}