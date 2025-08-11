# Guidelines for Junie AI Agent

# Final validation before submitting changes:

- Run `go vet ./...` and make sure it passes
- Run `golangci-lint run ./...` and make sure all reported issues are fixed
- Run `go build ./...` and make sure it passes
- Run `go test ./...` and make sure there is not failing tests