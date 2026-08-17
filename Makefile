.DEFAULT_GOAL := help

.PHONY: help build release release-dry-run

help:
	@printf "Usage: make <target>\n\n"
	@printf "Targets:\n"
	@printf "  build             Build the project\n"
	@printf "  release           Create a release\n"
	@printf "  release-dry-run   Preview a release\n"

build:
	cargo build

release:
	./scripts/release.sh $(VERSION)

release-dry-run:
	./scripts/release.sh $(VERSION) --dry-run
