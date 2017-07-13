release:
	go get github.com/goreleaser/goreleaser
	goreleaser --rm-dist

.PHONY: release
