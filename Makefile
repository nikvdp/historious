.PHONY: release release-dry-run

release:
	./scripts/release.sh $(VERSION)

release-dry-run:
	./scripts/release.sh $(VERSION) --dry-run
