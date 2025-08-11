# Guidelines for Junie AI Agent

## Test coverage

- Try to achieve 100% test coverage whenever possible.
- If needed use in code proxy var functions to simulate errors when running tests

## Final validation before submitting changes:

- Run `go vet ./...` and make sure it passes
- Run `golangci-lint run ./...` and make sure all reported issues are fixed
- Run `go build ./...` and make sure it passes
- Run `go test ./...` and make sure there is no failing tests