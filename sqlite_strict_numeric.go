package dalgo2sql

import "fmt"

// sqliteEmitter carries immutable per-read compiler choices. There is no
// process-global switch or registered function, and legacy wrappers use false.
type sqliteEmitter struct{ strictNumbers bool }

func (emitter sqliteEmitter) numberCopies() int {
	if emitter.strictNumbers {
		return 4
	}
	return 3
}

// strictNumber uses SQLite's lazy CASE to raise a native integer-overflow error
// before a lossy REAL cast. Each expression appears four times; parameter copies
// must follow that same order. abs(int64min) is a fixed error expression, never
// abs of the input (which would itself overflow for a legitimate int64min).
func strictSQLiteNumber(expression string, cast bool) string {
	numeric := "(CASE WHEN " + expression + " BETWEEN -9007199254740991 AND 9007199254740991 THEN " + expression + " ELSE abs(-9223372036854775808) END)"
	if cast {
		numeric = "CAST(" + numeric + " AS REAL)"
	}
	return "(CASE WHEN typeof(" + expression + ") IN ('integer','real') THEN " + numeric + " ELSE " + expression + " END)"
}

func (emitter sqliteEmitter) strictAggregate(name, arg string, values []any, distinct string) (string, []any, error) {
	// Preserve the integer class through SUM. SQLite either accumulates exact
	// int64 or reports overflow; it cannot silently switch integer SUM to REAL.
	input := "CASE WHEN typeof(" + arg + ") IN ('integer','real') THEN " + strictSQLiteNumber(arg, false) + " ELSE NULL END"
	inputValues := repeatSQLArgs(values, 5)
	total := emitter.normalizeSQLNumber("SUM(" + distinct + input + ")")
	totalValues := repeatSQLArgs(inputValues, emitter.numberCopies())
	if name == "SUM" {
		return total, totalValues, nil
	}
	count := emitter.normalizeSQLNumber("COUNT(" + distinct + input + ")")
	result := "(" + total + " / " + count + ")"
	resultValues := append(totalValues, repeatSQLArgs(inputValues, emitter.numberCopies())...)
	return emitter.normalizeSQLNumber(result), repeatSQLArgs(resultValues, emitter.numberCopies()), nil
}

func (emitter sqliteEmitter) normalizedNumber(value any) (float64, error) {
	number, err := normalizedNumber(value)
	if err == nil && emitter.strictNumbers && (number <= -9007199254740992 || number >= 9007199254740992) {
		return 0, fmt.Errorf("strict SQLite numeric parameter is outside the exact range")
	}
	return number, err
}
