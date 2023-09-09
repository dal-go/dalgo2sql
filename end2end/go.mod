module github.com/dal-go/dalgo2sql/end2end

go 1.20.0

require (
	github.com/dal-go/dalgo v0.10.0
	github.com/dal-go/dalgo-end2end-tests v0.0.31
	github.com/dal-go/dalgo2sql v0.0.8
	github.com/mattn/go-sqlite3 v1.14.17
)

replace github.com/dal-go/dalgo2sql v0.0.8 => ./../

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/georgysavva/scany/v2 v2.0.0 // indirect
	github.com/jackc/pgx/v5 v5.3.1 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/testify v1.8.4 // indirect
	github.com/strongo/random v0.0.1 // indirect
	github.com/strongo/validation v0.0.5 // indirect
	golang.org/x/crypto v0.8.0 // indirect
	golang.org/x/sys v0.7.0 // indirect
	golang.org/x/text v0.9.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)