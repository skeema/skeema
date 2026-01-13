package tengo

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

type introspector struct {
	// used for error propagation, context cancellation, and coordination/blocking
	g *errgroup.Group

	// instance being introspected
	instance *Instance

	// pre-obtained connection pool to instance, using instance.introspectionParams
	// but no default database; also limits concurrency via SetMaxOpenConns
	db *sql.DB

	// schema being populated; introspector tasks directly mutate this value's fields
	schema *Schema
}

// Go runs a subtask in a separate goroutine, using the same errgroup as the
// introspector.
func (insp *introspector) Go(ctx context.Context, task introspectorTask) {
	insp.g.Go(func() error {
		return task(ctx, insp)
	})
}

type introspectorTask func(context.Context, *introspector) error

type introspectorFixup func(schema *Schema, flavor Flavor)

// Globals that store top-level functions for tasks, fixups, and non-BASE TABLE
// table types. Values are added to these slices/maps via init() functions in
// other files in this package, to keep introspection logic for each object
// type in one place.
var (
	// primaryIntrospectorTasks are top-level introspection tasks, each handling a
	// different object type. They are run concurrently, and may spawn sub-tasks by
	// calling introspector.Go(ctx, subtask).
	primaryIntrospectorTasks []introspectorTask

	// primaryIntrospectorFixups are top-level tasks which are run after all of
	// the primaryIntrospectorTasks have returned. These are useful for cleaning up
	// discrepancies between data sources, e.g. cases where information_schema and
	// SHOW CREATE do not agree. They are run sequentially.
	primaryIntrospectorFixups []introspectorFixup

	// altTableTypeTasks are subtasks that are only called if at least one row in
	// information_schema.tables has a table_type equal to the map key. For
	// example, once MariaDB sequences are supported, this provides a mechanism
	// for running the sequence introspectorTask, but only if any sequences are
	// actually present in the schema. Note: no map entry for "BASE TABLE" is
	// needed since this is the default table type.
	altTableTypeTasks = map[string]introspectorTask{}
)

// IntrospectionOptions configures the behavior of IntrospectSchemas.
type IntrospectionOptions struct {
	SchemaNames    []string // optional; defaults to all non-system schemas on the Instance
	MaxConcurrency int      // optional; default 20
}

// IntrospectSchemas queries information_schema and various SHOW commands to
// build an in-memory representation of one or more schemas.
func IntrospectSchemas(ctx context.Context, instance *Instance, opts IntrospectionOptions) ([]*Schema, error) {
	g, ctx := errgroup.WithContext(ctx)
	db, err := instance.CachedConnectionPool("", instance.introspectionParams())
	if err != nil {
		return nil, err
	}

	// Determine list of schemas first
	var schemas []*Schema
	var query string
	var binds []any
	if len(opts.SchemaNames) == 0 {
		query = `
			SELECT schema_name, default_character_set_name, default_collation_name
			FROM   information_schema.schemata
			WHERE  schema_name NOT IN ('information_schema', 'performance_schema', 'mysql', 'test', 'sys')`
	} else {
		// If instance is using lower_case_table_names=2, apply an explicit collation
		// to ensure the schema name comes back with its original lettercasing. See
		// https://dev.mysql.com/doc/refman/8.0/en/charset-collation-information-schema.html
		var lctn2Collation string
		if instance.NameCaseMode() == NameCaseInsensitive {
			lctn2Collation = " COLLATE utf8_general_ci"
		}
		placeholders := make([]string, len(opts.SchemaNames))
		for n := range opts.SchemaNames {
			placeholders[n] = "?"
		}
		query = `
			SELECT schema_name, default_character_set_name, default_collation_name
			FROM   information_schema.schemata
			WHERE  schema_name` + lctn2Collation + ` IN (` + strings.Join(placeholders, ",") + `)`
		binds = make([]any, len(opts.SchemaNames))
		for n := range opts.SchemaNames {
			binds[n] = opts.SchemaNames[n]
		}
	}
	rows, err := db.QueryContext(ctx, query, binds...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var schema Schema
		if err := rows.Scan(&schema.Name, &schema.CharSet, &schema.Collation); err != nil {
			return nil, err
		}
		schemas = append(schemas, &schema)
	}
	if err = rows.Err(); err != nil || len(schemas) == 0 {
		return schemas, err
	}

	// Concurrency is limited using SetMaxOpenConns, as this approach minimizes
	// idle conns. If latency is low, the idle conn limit is kept at its default
	// (2 per database/sql), so most introspection conns get closed once there are
	// no more queries to run for this IntrospectSchemas call. However, if network
	// latency is nontrivial, the idle settings are adjusted to keep all conns open
	// (up to the 10 sec idle time limit per Instance.rawConnectionPool) for
	// potential reuse in a subsequent call to IntrospectSchemas. This is helpful
	// because many Skeema commands make separate IntrospectSchemas calls per
	// subdir, and/or separately introspect both a temp-schema workspace and a
	// "real" schema on the same server shortly afterwards.
	//
	// Idle behavior is similarly tweaked for integration tests to permit re-use
	// within each test, and also between different tests. This helps to avoid a
	// pileup of TIME_WAIT closed connections exhausting all ephemeral ports.
	concurrency := opts.MaxConcurrency
	if concurrency == 0 {
		concurrency = 20
	}
	if concurrency < instance.maxConnsPerPool() {
		db.SetMaxOpenConns(concurrency)
	}
	if instance.BaseLatency() > 2*time.Millisecond || testing.Testing() {
		db.SetMaxIdleConns(min(concurrency, instance.maxConnsPerPool()))
	}

	// Introspect objects in each schema
	for _, schema := range schemas {
		insp := &introspector{
			g:        g,
			instance: instance,
			db:       db,
			schema:   schema,
		}
		for _, baseTask := range primaryIntrospectorTasks {
			g.Go(func() error {
				return baseTask(ctx, insp)
			})
		}
	}

	err = g.Wait()
	if err == nil {
		flavor := instance.Flavor()
		for _, schema := range schemas {
			for _, baseFixup := range primaryIntrospectorFixups {
				baseFixup(schema, flavor)
			}
		}
	}

	return schemas, err
}

func showCreateObject(ctx context.Context, db *sql.DB, schema string, typ ObjectType, name string) (string, error) {
	var createStatement sql.NullString
	query := "SHOW CREATE " + typ.Caps() + " " + EscapeIdentifier(schema) + "." + EscapeIdentifier(name)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var dests []any
	if colNames, err := rows.Columns(); err != nil {
		return "", err
	} else {
		// Determine which column has the CREATE statement; all others scan into
		// rawbytes (we don't need them, but there's no syntax to skip them...)
		dests = make([]any, len(colNames))
		for n, colName := range colNames {
			if strings.HasPrefix(colName, "Create ") || colName == "SQL Original Statement" {
				dests[n] = &createStatement
			} else {
				var d sql.RawBytes
				dests[n] = &d
			}
		}
	}
	if rows.Next() {
		err = rows.Scan(dests...)
	} else {
		err = rows.Err()
	}
	if err == nil && createStatement.String == "" {
		err = sql.ErrNoRows
	}
	// For stored objects, normalize newlines; also trim any trailing semicolon
	// which may be present if object was created with multiStatements enabled
	if typ != ObjectTypeTable {
		createStatement.String = strings.TrimSuffix(strings.ReplaceAll(createStatement.String, "\r\n", "\n"), ";")
	}
	return createStatement.String, err
}
