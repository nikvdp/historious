.DEFAULT_GOAL := help

.PHONY: help build release-build install prepare-release release-dry-run

help:
	@printf "Usage: make <target>\n\n"
	@printf "Targets:\n"
	@printf "  build             Build the project\n"
	@printf "  release-build     Build the project in release mode\n"
	@printf "  install           Build in release mode and install to PATH\n"
	@printf "  prepare-release   Create a release\n"
	@printf "  release-dry-run   Preview a release\n"

build:
	cargo build

release-build:
	cargo build --release

install: release-build
	@if dest=$$(which histo 2>/dev/null); then \
		cp target/release/histo "$$dest"; \
	else \
		mkdir -p ~/.local/bin && cp target/release/histo ~/.local/bin/histo; \
	fi

prepare-release:
	./scripts/release.sh $(VERSION)

release-dry-run:
	./scripts/release.sh $(VERSION) --dry-run
