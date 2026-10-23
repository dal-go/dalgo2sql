package dalgo2sql

import "fmt"

const (
	// dialectMySQL is the StructuredQueryDialect of MySQL: structured reads go through
	// the typed compiler with mysqlDialect.
	dialectMySQL = "mysql"
	// dialectOracle is the StructuredQueryDialect of Oracle Database: structured reads
	// go through the typed compiler with oracleDialect.
	dialectOracle = "oracle"
	// dialectSQLServer is the StructuredQueryDialect of SQL Server: structured reads go
	// through the typed compiler with sqlserverDialect.
	dialectSQLServer = "sqlserver"
)

// isTypedDialect reports whether a StructuredQueryDialect is compiled by the typed
// compiler (compileTypedSQL), which reads the catalog facts of every source first.
func isTypedDialect(name string) bool {
	switch name {
	case dialectPostgres, dialectMySQL, dialectOracle, dialectSQLServer:
		return true
	}
	return false
}

// typedEngineName is the engine's name in an error message.
func typedEngineName(dialect string) string {
	switch dialect {
	case dialectMySQL:
		return "MySQL"
	case dialectOracle:
		return "Oracle"
	case dialectSQLServer:
		return "SQL Server"
	}
	return "PostgreSQL"
}

// typedDialectFor builds the typed dialect DbOptions asks for. IdentifierCase is read
// by PostgreSQL only; MySQL, Oracle and SQL Server write every name as the query spells it, so any
// value but exact is refused for them rather than ignored: a mount that meant to fold
// names must not run unfolded.
func typedDialectFor(options DbOptions) (typedDialect, error) {
	switch options.StructuredQueryDialect {
	case dialectPostgres:
		dialect, err := postgresDialectFor(options)
		if err != nil {
			return nil, err
		}
		return dialect, nil
	case dialectMySQL, dialectOracle, dialectSQLServer:
		if options.IdentifierCase != "" && options.IdentifierCase != IdentifierCaseExact {
			return nil, fmt.Errorf("unsupported identifier case %q for the %s dialect: only %q is supported", options.IdentifierCase, options.StructuredQueryDialect, IdentifierCaseExact)
		}
		return map[string]typedDialect{dialectMySQL: mysqlDialect{}, dialectOracle: oracleDialect{}, dialectSQLServer: sqlserverDialect{}}[options.StructuredQueryDialect], nil
	}
	return nil, fmt.Errorf("unsupported structured query dialect %q", options.StructuredQueryDialect)
}
