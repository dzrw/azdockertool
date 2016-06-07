SHELL=/bin/bash

name := azdockertool
version := $(shell cat VERSION)
command := bin/$(name)

.PHONY: build

build:
		gb build

linux:
		env GOOS=linux GOARCH=amd64 gb build
		@md5 bin/azdockertool-linux-amd64

test:
		gb test

version: build
		$(command) version

scp: linux
		@docker-machine scp bin/$(name)-linux-amd64 coreos1010:/home/docker/.local/bin/azdockertool