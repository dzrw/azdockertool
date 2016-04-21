SHELL=/bin/bash

name := azdockertool
version := $(shell cat VERSION)
command := bin/$(name)

.PHONY: build

build:
		gb build

test:
		gb test

version: build
		$(command) version
