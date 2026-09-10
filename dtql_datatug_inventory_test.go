package dalgo2sql

import (
	"strings"
	"testing"

	"github.com/dal-go/dalgo/condeval"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dtql"
)

// TestDatatugShippedDTQLQueriesCompile is S114 Stage 1 step 2's inventory:
// every DTQL-YAML text datatug actually ships or tests, deserialized with
// dal-go/dalgo's own dtql.Deserialize and run through this package's
// compileStructuredSQL, the same two steps datatug-cli's protected sqlite
// read path performs once StructuredQueryDialect: "sqlite" is opted in
// (pkg/dbcopy/url.go OpenProtected). Sources, at the time this test was
// written (2026-09-10):
//
//   - datatug/datatug-demo-projects demo-project-1 is the ONLY datatug repo
//     under /Users/alex/projects/datatug with a *.query.dtql file anywhere
//     in it (`find ... -iname '*.query.dtql'`, org-wide): exactly one,
//     queries/customers/customer-invoices.query.dtql.
//   - datatug/datatug-cli has no *.dtql fixture files; its structured-query
//     tests embed DTQL-YAML as Go string constants. Every `from:`-rooted
//     constant in pkg/server, pkg/server/endpoints and pkg/secureread is
//     reproduced verbatim below (`grep -rln '`from:' --include='*.go'`
//     across datatug-cli found exactly pkg/dbcopy/url.go [a doc comment,
//     not a fixture], pkg/server/security_matrix_test.go,
//     pkg/server/security_matrix_task13_test.go and
//     pkg/server/endpoints/exec_run_query_test.go).
//
// Each case names its compile verdict; a case documented "rejected" is a
// pre-existing, deliberate restriction this dialect keeps (column alias
// policy lives one layer up, in datatug-cli's pkg/accesspolicies — see the
// case comment) rather than something Stage 1 needed to fix.
func TestDatatugShippedDTQLQueriesCompile(t *testing.T) {
	cases := []struct {
		name string
		// source cites the exact file (and, for embedded fixtures, the Go
		// constant) this query text was copied from.
		source  string
		dtql    string
		wantErr string // "" means compile must succeed
	}{
		{
			name: "demo-project-1 customer-invoices (the query this stream exists for)",
			source: "datatug/datatug-demo-projects demo-project-1 " +
				"queries/customers/customer-invoices.query.dtql",
			dtql: `from:
  name: Invoice
  alias: i
columns:
  - field: InvoiceId
  - field: InvoiceDate
  - field: BillingCity
  - field: BillingCountry
  - field: Total
where:
  op: ==
  left:
    field: CustomerId
  right:
    param: CustomerId
orderBy:
  - field: InvoiceDate
    desc: true
`,
			wantErr: "",
		},
		{
			name:   "datatug-cli runQueryTestCustomerByIDDTQL (wildcard select, no alias)",
			source: "datatug/datatug-cli pkg/server/endpoints/exec_run_query_test.go:runQueryTestCustomerByIDDTQL",
			dtql: `from:
  name: Customer
where:
  op: "=="
  left:
    field: CustomerId
  right:
    param: CustomerId
`,
			wantErr: "",
		},
		{
			name:   "datatug-cli customerByIDDTQL (wildcard select, no alias)",
			source: "datatug/datatug-cli pkg/server/security_matrix_test.go:customerByIDDTQL",
			dtql: `from:
  name: Customer
where:
  op: "=="
  left:
    field: CustomerId
  right:
    param: CustomerId
`,
			wantErr: "",
		},
		{
			name:   "datatug-cli customerEmailExplicitDTQL (explicit hidden-column probe, no alias)",
			source: "datatug/datatug-cli pkg/server/security_matrix_test.go:customerEmailExplicitDTQL",
			dtql: `from:
  name: Customer
columns:
  - field: CustomerId
  - field: Email
where:
  op: "=="
  left:
    field: CustomerId
  right:
    param: CustomerId
`,
			wantErr: "",
		},
		{
			// This is a COLUMN alias ("as: display_name"), not a FROM-source
			// alias — a different DTQL feature from the one this stream
			// adds. The dialect has always compiled column aliases (see
			// TestCompileStructuredSQLParameterizedAndQuoted's `display"name`
			// case); datatug-cli's own security_matrix_task13_test.go
			// documents that this shape is refused one layer up, by
			// pkg/accesspolicies.Run's ErrInvalidQuery, before any SQL
			// dialect ever sees it. Listed here to record that the dialect
			// itself does not reject it, so nobody mistakes this repo for
			// the enforcement point.
			name:   "datatug-cli customerAliasedColumnDTQL (COLUMN alias, not source alias; refused upstream by accesspolicies, not by this dialect)",
			source: "datatug/datatug-cli pkg/server/security_matrix_task13_test.go:customerAliasedColumnDTQL",
			dtql: `from:
  name: Customer
columns:
  - field: CustomerId
  - field: FirstName
    as: display_name
where:
  op: "=="
  left:
    field: CustomerId
  right:
    param: CustomerId
`,
			wantErr: "",
		},
		{
			name:   "datatug-cli customerInvoiceDTQL (wildcard select, no alias)",
			source: "datatug/datatug-cli pkg/server/security_matrix_task13_test.go:customerInvoiceDTQL",
			dtql: `from:
  name: Invoice
where:
  op: "=="
  left:
    field: CustomerId
  right:
    param: CustomerId
`,
			wantErr: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q, err := dtql.Deserialize([]byte(tc.dtql))
			if err != nil {
				t.Fatalf("dtql.Deserialize(%s): %v", tc.source, err)
			}
			// Every case above binds its `param:` node to CustomerId via
			// datatug-cli's own pkg/accesspolicies.Run -> substituteParams,
			// which resolves it to a dal.Constant with condeval.Substitute
			// BEFORE the query ever reaches dal.ReadSession.
			// ExecuteQueryToRecordsReader (and so, transitively,
			// compileStructuredSQL) — the structured dialect itself only
			// ever sees FieldRef/Constant, never a raw dal.Param. Reproduce
			// that same substitution here so this inventory compiles what
			// dalgo2sql actually receives in production, not a shape it
			// never sees.
			if q.Where() != nil {
				resolved, err := condeval.Substitute(q.Where(), func(name string) (any, bool) {
					if name == "CustomerId" {
						return 3, true
					}
					return nil, false
				})
				if err != nil {
					t.Fatalf("condeval.Substitute(%s): %v", tc.source, err)
				}
				q = dal.WithWhere(q, resolved)
			}
			_, _, err = compileStructuredSQL(q)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("compileStructuredSQL(%s) = %v, want success", tc.source, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("compileStructuredSQL(%s) = nil error, want rejection containing %q", tc.source, tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("compileStructuredSQL(%s) error = %q, want it to contain %q", tc.source, err.Error(), tc.wantErr)
			}
		})
	}
}
