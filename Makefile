.PHONY: build check test race vet examples privacy hooks menubar menubar-check
SOURCE_HASH := $(shell shasum -a 256 *.go go.mod | shasum -a 256 | cut -d ' ' -f 1)
build:
	mkdir -p bin
	go build -trimpath -ldflags "-X main.buildSourceHash=$(SOURCE_HASH)" -o bin/claude-proxy .
test:
	go test ./... -timeout 120s
race:
	go test -race ./... -timeout 120s
vet:
	go vet ./...
examples:
	go test ./... -run '^TestConfiguredExampleConfigurations$$'
privacy:
	python3 -B -m unittest discover -s scripts -p 'test_*.py'
	python3 -B scripts/privacy_check.py --staged --history
hooks:
	git config --local core.hooksPath .githooks
check: privacy test race vet examples
menubar:
	sh menubar/build.sh
menubar-check:
	swift test --package-path menubar
	plutil -lint menubar/Info.plist
