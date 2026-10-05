package dalgo2sql

import (
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
)

// FIRST and LAST need a provider-declared stable input order, which no dialect of this
// adapter declares, so DALgo's planner refuses a query that uses one for every dialect:
// the README says so, and nothing falls back to the generic engine.
func TestDALgoPlannerRefusesFirstAndLastForEveryDialectOfTheAdapter(t *testing.T) {
	for _, dialect := range []string{"", "sqlite", "postgres"} {
		for _, function := range []string{dal.FIRST, dal.LAST} {
			t.Run(dialect+"/"+function, func(t *testing.T) {
				q := typedTestFrom("Album", "").NewQuery().GroupBy(typedTestField("Title")).
					SelectColumns(typedTestColumn(typedTestField("Title"), ""),
						dal.Column{Expression: dal.NewAggregate(function, false, typedTestField("AlbumId")), Alias: "x"})
				capabilities := (&database{options: DbOptions{StructuredQueryDialect: dialect}}).QueryCapabilities()
				plan, err := dal.PlanAggregation(q, capabilities)
				if err == nil || !strings.Contains(err.Error(), "FIRST/LAST require a provider-declared stable input order") {
					t.Fatalf("PlanAggregation() = %+v, %v; want the planner's refusal", plan, err)
				}
			})
		}
	}
}
