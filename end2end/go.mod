module github.com/dal-go/dalgo2sql/end2end

go 1.26.0

toolchain go1.27.2

require (
	github.com/dal-go/dalgo v0.93.4
	github.com/dal-go/dalgo2sql v0.9.6 // No version as we alway replace it with local version
	github.com/mattn/go-sqlite3 v1.14.52
)

replace github.com/dal-go/dalgo2sql => ./../

require (
	github.com/RoaringBitmap/roaring/v2 v2.29.0 // indirect
	github.com/bits-and-blooms/bitset v1.24.6 // indirect
	github.com/dal-go/record v0.1.4 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/georgysavva/scany/v2 v2.1.4 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgx/v5 v5.7.6 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/stretchr/testify v1.12.1 // indirect
	github.com/strongo/random v0.0.3 // indirect
	github.com/strongo/validation v0.0.15 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/crypto v0.46.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	modernc.org/libc v1.77.1 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.12.1 // indirect
	modernc.org/sqlite v1.60.1 // indirect
)
