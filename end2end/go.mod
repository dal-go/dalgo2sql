module github.com/dal-go/dalgo2sql/end2end

go 1.24.0

toolchain go1.26.5

require (
	github.com/dal-go/dalgo v0.62.9
	github.com/dal-go/dalgo2sql v0.0.0 // No version as we alway replace it with local version
	github.com/mattn/go-sqlite3 v1.14.47
)

replace github.com/dal-go/dalgo2sql => ./../

require (
	github.com/RoaringBitmap/roaring/v2 v2.19.0 // indirect
	github.com/bits-and-blooms/bitset v1.24.4 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/georgysavva/scany/v2 v2.1.4 // indirect
	github.com/jackc/pgx/v5 v5.7.6 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	github.com/strongo/random v0.0.1 // indirect
	github.com/strongo/validation v0.0.9 // indirect
	golang.org/x/crypto v0.46.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
