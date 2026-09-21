package dalgo2sql

import (
	"fmt"

	"github.com/dal-go/dalgo/dal"
)

type wildcardProjectionPlan struct {
	qualifier     string
	projection    *dal.WildcardProjection
	explicitCount int
}

func planWildcardProjection(q dal.StructuredQuery) (*wildcardProjectionPlan, error) {
	columns := q.Columns()
	var projection *dal.WildcardProjection
	for i, column := range columns {
		if column.Wildcard == nil {
			continue
		}
		if projection != nil {
			return nil, fmt.Errorf("multiple wildcard projections require join support")
		}
		if i != 0 {
			return nil, fmt.Errorf("wildcard projection must precede explicit columns")
		}
		if column.Expression != nil {
			return nil, fmt.Errorf("wildcard projection cannot also contain an expression")
		}
		if column.Alias != "" {
			return nil, fmt.Errorf("wildcard projection cannot have an alias")
		}
		projection = column.Wildcard
	}
	if projection == nil {
		return nil, nil
	}
	if len(projection.Exclude) == 0 {
		return nil, fmt.Errorf("wildcard projection requires at least one exclusion")
	}
	if q.From() == nil || q.From().Base() == nil {
		return nil, fmt.Errorf("wildcard projection requires a source")
	}
	if len(q.From().Joins()) != 0 {
		return nil, fmt.Errorf("wildcard projection with joins is not supported")
	}
	base := q.From().Base()
	qualifier := ""
	if projection.Source != "" {
		if projection.Source != base.Alias() && projection.Source != base.Name() {
			return nil, fmt.Errorf("wildcard projection source %q does not match source name or alias", projection.Source)
		}
		qualifier = base.Alias()
		if qualifier == "" {
			qualifier = base.Name()
		}
	}
	for i, name := range projection.Exclude {
		if name == "" {
			return nil, fmt.Errorf("wildcard projection exclusion %d is empty", i)
		}
	}
	return &wildcardProjectionPlan{
		qualifier:     qualifier,
		projection:    projection,
		explicitCount: len(columns) - 1,
	}, nil
}

func (p wildcardProjectionPlan) sqlExpression(quote func(string) string) string {
	if p.qualifier == "" {
		return "*"
	}
	return quote(p.qualifier) + ".*"
}

func (p wildcardProjectionPlan) visibleIndexes(columnNames []string) ([]int, error) {
	wildcardEnd := len(columnNames) - p.explicitCount
	if wildcardEnd < 0 {
		return nil, fmt.Errorf("SQL result has %d columns, fewer than %d explicit projections", len(columnNames), p.explicitCount)
	}
	indexes := make([]int, 0, len(columnNames))
	for i, name := range columnNames {
		if i < wildcardEnd && p.projection.Excludes(name) {
			continue
		}
		indexes = append(indexes, i)
	}
	return indexes, nil
}
