GOTEST := go test
TAGS ?= integration
COVERAGE_FILE := coverage.out

.PHONY: codespell
codespell:
	@echo "Running codespell"
	@codespell

.PHONY: test
test:
	@echo "Running tests with tags: $(TAGS)"
	@go test ./... -count=1 -v -tags="$(TAGS)"

.PHONY: testrace
testrace:
	@echo "Running tests with race flag"
	@go test ./... -count=100 -race -v

.PHONY: coverage
coverage:
	@echo "Running tests with coverage and tags: $(TAGS)"
	go test -tags "$(TAGS)" $(shell go list ./... | grep -v test_helpers) \
		-v -coverprofile=$(COVERAGE_FILE).tmp -count=1 -covermode=atomic
	cat $(COVERAGE_FILE).tmp | grep -v 'internal/mocks/' | grep -v 'internal/testing/' > $(COVERAGE_FILE)
	go tool cover -func=$(COVERAGE_FILE)

.PHONY: coveralls
coveralls:
	@echo "uploading coverage to coveralls"
	@goveralls -coverprofile=$(COVERAGE_FILE) -service=github

.PHONY: coveralls-deps
coveralls-deps:
	@echo "Installing coveralls"
	@go get github.com/mattn/goveralls
	@go install github.com/mattn/goveralls

.PHONY: lint-deps
lint-deps:
	@echo "Installing lint deps"
	@go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.10.1

.PHONY: generate
generate:
	@echo "Running go generate"
	@go generate ./...

.PHONY: lint
lint:
	@echo "Running go-linter"
	@go mod tidy
	@go mod vendor
	@golangci-lint run --config=./.golangci.yml --modules-download-mode vendor
