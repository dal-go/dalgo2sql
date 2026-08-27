module github.com/dal-go/dalgo2sql/end2end

go 1.26.0

toolchain go1.27.0

require (
	github.com/dal-go/dalgo v0.74.1
	github.com/dal-go/dalgo2sql v0.9.6 // No version as we alway replace it with local version
	github.com/mattn/go-sqlite3 v1.14.50
)

replace github.com/dal-go/dalgo2sql => ./../

require (
	github.com/RoaringBitmap/roaring/v2 v2.25.0 // indirect
	github.com/bits-and-blooms/bitset v1.24.6 // indirect
	github.com/dal-go/record v0.1.3 // indirect
	github.com/georgysavva/scany/v2 v2.1.4 // indirect
	github.com/jackc/pgx/v5 v5.7.6 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/stretchr/testify v1.12.1 // indirect
	github.com/strongo/random v0.0.1 // indirect
	github.com/strongo/validation v0.0.10 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/crypto v0.46.0 // indirect
	golang.org/x/text v0.32.0 // indirect
)
