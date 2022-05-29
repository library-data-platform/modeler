package modeler

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v4"
)

type Model struct {
	tables []tableschema
	refs   []reference
}

func NewModel(connString string, msg func(string)) (*Model, error) {
	var err error
	var model = new(Model)
	var dbc = new(dbconn)
	if dbc.Conn, err = pgx.Connect(context.TODO(), connString); err != nil {
		return nil, fmt.Errorf("connecting to database: %v", err)
	}
	defer dbc.Conn.Close(context.TODO())
	dbc.ConnString = connString
	// Read list of tables.
	msg("reading schema")
	if model.tables, err = getTableList(dbc); err != nil {
		return nil, fmt.Errorf("reading table list: %v", err)
	}
	// Read foreign keys.
	var t tableschema
	for _, t = range model.tables {
		msg("scanning table: " + t.SchemaName + "." + t.TableName)
		if err = searchTableForeignKeys(dbc, model.tables, t, &(model.refs)); err != nil {
			return nil, fmt.Errorf("scanning for foreign keys: %s.%s: %v", t.SchemaName, t.TableName, err)
		}
	}
	return model, nil
}

func (m *Model) EncodeDOT() string {
	var b strings.Builder
	b.WriteString("digraph G {\n")
	b.WriteString("    pad=0.5; nodesep=0.5; ranksep=2; rankdir=\"LR\"; ordering=\"out\"\n")
	b.WriteString("        node[shape=\"plain\"; fontname=\"Monospace\"];\n")
	var t tableschema
	for _, t = range m.tables {
		var tstr = t.SchemaName + "." + t.TableName
		b.WriteString("    \"" + tstr + "\" [label=")
		b.WriteString("<<table border=\"0\" cellborder=\"1\" cellpadding=\"4\" cellspacing=\"0\">")
		b.WriteString("<tr><td><b> " + tstr + " </b></td></tr>")
		var c columnschema
		for _, c = range t.Columns {
			b.WriteString("<tr><td port=\"" + c.Name + "\" align=\"left\"> " + c.Name + " </td></tr>")
		}
		b.WriteString("</table>>];\n")
	}
	var r reference
	for _, r = range m.refs {
		var c int = rand.Intn(len(palette))
		b.WriteString("    \"" + r.SourceTable + "\":\"" + r.SourceColumn + "\" -> \"" + r.TargetTable + "\":\"" + r.TargetColumn + "\" [style=\"bold\"; color=\"" + palette[c] + "\"];\n")
	}
	b.WriteString("}\n")
	return b.String()
}

func getTableList(dbc *dbconn) ([]tableschema, error) {
	var err error
	var tables []tableschema
	if tables, err = selectTableList(dbc); err != nil {
		return nil, fmt.Errorf("selecting table list: %v", err)
	}
	// Read column schemas.
	var i int
	var t tableschema
	for i, t = range tables {
		// fmt.Fprintf(os.Stderr, "%s.%s\n", t.SchemaName, t.TableName)
		var columns []columnschema
		if columns, err = getColumnList(dbc, t.SchemaName, t.TableName); err != nil {
			return nil, fmt.Errorf("selecting column list: %v", err)
		}
		tables[i] = tableschema{
			SchemaName: t.SchemaName,
			TableName:  t.TableName,
			Columns:    columns,
		}
	}
	return tables, nil
}

func selectTableList(dbc *dbconn) ([]tableschema, error) {
	var err error
	// Read table schemas.
	var q = "SELECT schemaname, tablename FROM metadb.track WHERE tablename LIKE '%\\_\\_t' ORDER BY schemaname, tablename"
	var rows pgx.Rows
	if rows, err = dbc.Conn.Query(context.TODO(), q); err != nil {
		return nil, fmt.Errorf("executing query: %v", err)
	}
	var tables = make([]tableschema, 0)
	defer rows.Close()
	for rows.Next() {
		var s, t string
		if err = rows.Scan(&s, &t); err != nil {
			return nil, fmt.Errorf("scanning results: %v", err)
		}
		if strings.HasPrefix(t, "rmb_") {
			continue
		}
		tables = append(tables, tableschema{SchemaName: s, TableName: t})
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("scanning results: %v", rows.Err())
	}
	return tables, nil
}

func getColumnList(dbc *dbconn, schema, table string) ([]columnschema, error) {
	var err error
	var columns []columnschema
	if columns, err = selectColumnList(dbc, schema, table); err != nil {
		return nil, fmt.Errorf("selecting columns: %v", err)
	}
	var i int
	var c columnschema
	for i, c = range columns {
		// var k bool
		// if k, err = isKey(dbc, schema, table, c.Name); err != nil {
		// 	return nil, fmt.Errorf("checking for key: %v", err)
		// }
		columns[i] = columnschema{
			Name: c.Name,
			Num:  c.Num,
			// Key:  k,
		}
	}
	return columns, nil
}

func selectColumnList(dbc *dbconn, schema, table string) ([]columnschema, error) {
	var err error
	// Read column schemas.
	var q = "SELECT a.attname, a.attnum FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid JOIN pg_attribute a ON c.oid=a.attrelid WHERE n.nspname='" + schema + "' AND c.relname='" + table + "' AND a.attnum > 0 ORDER BY a.attnum"
	var rows pgx.Rows
	if rows, err = dbc.Conn.Query(context.TODO(), q); err != nil {
		return nil, fmt.Errorf("executing query: %v", err)
	}
	var columns = make([]columnschema, 0)
	defer rows.Close()
	for rows.Next() {
		var c string
		var n int16
		if err = rows.Scan(&c, &n); err != nil {
			return nil, fmt.Errorf("scanning results: %v", err)
		}
		if strings.HasPrefix(c, "__") {
			continue
		}
		if c == "jsonb" || c == "content" || c == "creation_date" || c == "created_by" {
			continue
		}
		columns = append(columns, columnschema{Name: c, Num: n})
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("scanning results: %v", rows.Err())
	}
	return columns, nil
}

func isKey(dbc *dbconn, schema, table, column string) (bool, error) {
	var err error
	var q = "SELECT 1 FROM " + schema + "." + table + " GROUP BY \"" + column + "\" HAVING count(*) > 1 LIMIT 1"
	var n int64
	err = dbc.Conn.QueryRow(context.TODO(), q).Scan(&n)
	switch {
	case err == pgx.ErrNoRows:
		return true, nil
	case err != nil:
		return false, err
	default:
		return false, nil
	}
}

func searchTableForeignKeys(dbc *dbconn, tables []tableschema, table tableschema, refs *[]reference) error {
	var err error
	var tablestr = table.SchemaName + "." + table.TableName
	var c columnschema
	for _, c = range table.Columns {
		if c.Num == 6 {
			continue
		}
		var sample string
		if sample, err = getSampleData(dbc, table.SchemaName, table.TableName, c.Name); err != nil {
			return fmt.Errorf("reading sample data: %v", err)
		}
		if !isUUID(sample) {
			continue
		}
		var t1 tableschema
		for _, t1 = range tables {
			var c1 columnschema
			for _, c1 = range t1.Columns {
				if c1.Num != 6 {
					continue
				}
				var ref = reference{
					SourceTable:  tablestr,
					SourceColumn: c.Name,
					TargetTable:  t1.SchemaName + "." + t1.TableName,
					TargetColumn: c1.Name,
				}
				if ref.SourceTable == ref.TargetTable {
					continue
				}
				var k bool
				if k, err = isForeignKey(dbc, tablestr, c.Name, t1.SchemaName+"."+t1.TableName, c1); err != nil {
					return fmt.Errorf("checking for foreign key: %v", err)
				}
				if k {
					*refs = append(*refs, ref)
					fmt.Fprintf(os.Stderr, "modeler: found: %s (%s) -> %s (%s)\n", ref.SourceTable, ref.SourceColumn, ref.TargetTable, ref.TargetColumn)
				}
			}
		}
	}
	return nil
}

func isForeignKey(dbc *dbconn, table2 string, column2 string, table1 string, column1 columnschema) (bool, error) {
	var err error
	var e bool
	if e, err = isTableEmpty(dbc, table2); err != nil {
		return false, fmt.Errorf("checking for empty table: %v: %v", table2, err)
	}
	if e {
		return false, nil
	}
	if e, err = isTableEmpty(dbc, table1); err != nil {
		return false, fmt.Errorf("checking for empty table: %v: %v", table1, err)
	}
	if e {
		return false, nil
	}
	// if !column1.Key {
	// 	return false, nil
	// }
	var q = "SELECT 1 FROM " + table2 + " r2 LEFT JOIN " + table1 + " r1 ON r2." + column2 + "=r1." + column1.Name + " WHERE r2." + column2 + " IS NOT NULL AND r1." + column1.Name + " IS NULL LIMIT 1"
	var n int64
	err = dbc.Conn.QueryRow(context.TODO(), q).Scan(&n)
	switch {
	case err == pgx.ErrNoRows:
		return true, nil
	case err != nil:
		return true, nil
	default:
		return false, nil
	}
}

func isTableEmpty(dbc *dbconn, table string) (bool, error) {
	var err error
	var q = "SELECT 1 FROM " + table + " LIMIT 1"
	var n int64
	err = dbc.Conn.QueryRow(context.TODO(), q).Scan(&n)
	switch {
	case err == pgx.ErrNoRows:
		return true, nil
	case err != nil:
		return false, err
	default:
		return false, nil
	}
}

func getSampleData(dbc *dbconn, schema, table, column string) (string, error) {
	var err error
	var q = "SELECT \"" + column + "\"::text FROM " + schema + "." + table + " LIMIT 1"
	var d *string
	err = dbc.Conn.QueryRow(context.TODO(), q).Scan(&d)
	switch {
	case err == pgx.ErrNoRows:
		return "", nil
	case err != nil:
		return "", err
	default:
		if d == nil {
			return "", nil
		} else {
			return *d, nil
		}
	}
}

type tableschema struct {
	SchemaName string
	TableName  string
	Columns    []columnschema
}

func (t tableschema) String() string {
	var b strings.Builder
	b.WriteString(t.SchemaName + "." + t.TableName + ":")
	var c columnschema
	for _, c = range t.Columns {
		b.WriteString(" " + c.String())
	}
	b.WriteRune('\n')
	return b.String()
}

type columnschema struct {
	Name string
	Num  int16
	Key  bool
}

func (c columnschema) String() string {
	if c.Key {
		return c.Name + "*"
	} else {
		return c.Name
	}
}

type reference struct {
	SourceTable  string
	SourceColumn string
	TargetTable  string
	TargetColumn string
}

func (r reference) String() string {
	return r.SourceTable + "(" + r.SourceColumn + ") -> " + r.TargetTable + "(" + r.TargetColumn + ")\n"
}

type dbconn struct {
	Conn       *pgx.Conn
	ConnString string
}

func isUUID(str string) bool {
	return uuidRegexp.MatchString(str)
}

var uuidRegexp = regexp.MustCompile(`^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$`)
