package dalgo2sql

import "strings"

// The words a SQL engine rejects where a column name stands bare, by engine. The legacy
// text emitter writes names unquoted for any engine, so a keys-only query is refused when
// its primary key is a word any of these engines reserves (reservedSQLWords is their
// union): the same text must run on each of them, and a word one engine accepts bare
// (index, set and update on PostgreSQL, user on SQLite) is refused because another does not.
//
// The lists are the documented reserved keywords: PostgreSQL 17 (Table C.1: reserved, and
// reserved as a function or type name, which a column name cannot be either), MySQL 8.4
// (reserved), SQL Server (T-SQL reserved keywords), and the words of SQLite's keyword list
// that its parser does not let stand for a name (the others are accepted as names through
// its fallback). They are conservative, not complete: a name outside the union can still
// be refused by the server of an engine whose list grows.

const postgresReservedWords = `all analyse analyze and any array as asc asymmetric both case cast check collate column
constraint create current_catalog current_date current_role current_time current_timestamp
current_user default deferrable desc distinct do else end except false fetch for foreign from
grant group having in initially intersect into lateral leading limit localtime localtimestamp
not null offset on only or order placing primary references returning select session_user some
symmetric system_user table then to trailing true union unique user using variadic when where
window with authorization binary collation concurrently cross current_schema freeze full ilike
inner is isnull join left like natural notnull outer overlaps right similar tablesample verbose`

const mysqlReservedWords = `accessible add all alter analyze and as asc asensitive before between bigint binary blob
both by call cascade case change char character check collate column condition constraint
continue convert create cross cube cume_dist current_date current_time current_timestamp
current_user cursor database databases day_hour day_microsecond day_minute day_second dec
decimal declare default delayed delete dense_rank desc describe deterministic distinct
distinctrow div double drop dual each else elseif empty enclosed escaped except exists exit
explain false fetch first_value float float4 float8 for force foreign from fulltext function
generated get grant group grouping groups having high_priority hour_microsecond hour_minute
hour_second if ignore in index infile inner inout insensitive insert int int1 int2 int3 int4
int8 integer intersect interval into io_after_gtids io_before_gtids is iterate join json_table
key keys kill lag last_value lateral lead leading leave left like limit linear lines load
localtime localtimestamp lock long longblob longtext loop low_priority master_bind
master_ssl_verify_server_cert match maxvalue mediumblob mediumint mediumtext middleint
minute_microsecond minute_second mod modifies natural not no_write_to_binlog nth_value ntile
null numeric of on optimize optimizer_costs option optionally or order out outer outfile over
partition percent_rank precision primary procedure purge range rank read reads read_write real
recursive references regexp release rename repeat replace require resignal restrict return
revoke right rlike row rows row_number schema schemas second_microsecond select sensitive
separator set show signal smallint spatial specific sql sqlexception sqlstate sqlwarning
sql_big_result sql_calc_found_rows sql_small_result ssl starting stored straight_join system
table terminated then tinyblob tinyint tinytext to trailing trigger true undo union unique
unlock unsigned update usage use using utc_date utc_time utc_timestamp values varbinary
varchar varcharacter varying virtual when where while window with write xor year_month
zerofill`

const sqlServerReservedWords = `add all alter and any as asc authorization backup begin between break browse bulk by
cascade case check checkpoint close clustered coalesce collate column commit compute
constraint contains containstable continue convert create cross current current_date
current_time current_timestamp current_user cursor database dbcc deallocate declare default
delete deny desc disk distinct distributed double drop dump else end errlvl escape except exec
execute exists exit external fetch file fillfactor for foreign freetext freetexttable from
full function goto grant group having holdlock identity identity_insert identitycol if in
index inner insert intersect into is join key kill left like lineno load merge national
nocheck nonclustered not null nullif of off offsets on open opendatasource openquery
openrowset openxml option or order outer over percent pivot plan precision primary print proc
procedure public raiserror read readtext reconfigure references replication restore restrict
return revert revoke right rollback rowcount rowguidcol rule save schema securityaudit select
semantickeyphrasetable semanticsimilaritydetailstable semanticsimilaritytable session_user set
setuser shutdown some statistics system_user table tablesample textsize then to top tran
transaction trigger truncate try_convert tsequal union unique unpivot update updatetext use
user values varying view waitfor when where while with writetext`

const sqliteReservedWords = `add all alter and as autoincrement between case check collate commit constraint create
cross current_date current_time current_timestamp default deferrable delete distinct drop else
escape except exists filter foreign from full glob group having in index indexed inner insert
intersect into is isnull join left limit natural not nothing notnull null on or order outer
over primary references regexp returning right select set table then to transaction union
unique update using values when where window`

// reservedSQLWords is the union of the lists above, in lower case: the words an engine
// rejects as a bare name in the text of a statement. It is a conservative list, not any one
// engine's.
var reservedSQLWords = wordSet(postgresReservedWords, mysqlReservedWords, sqlServerReservedWords, sqliteReservedWords)

// wordSet is the set of the words of lists, each a text of words separated by white space.
func wordSet(lists ...string) map[string]bool {
	words := map[string]bool{}
	for _, list := range lists {
		for _, word := range strings.Fields(list) {
			words[word] = true
		}
	}
	return words
}
