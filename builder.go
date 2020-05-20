package sqlee

// SelectBuilder defines a function for query-builders to output their SQL
// query and the corresponding arguments
type SelectBuilder interface {
	ToSql() (query string, args []interface{}, err error)
}
