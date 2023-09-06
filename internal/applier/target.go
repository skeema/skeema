package applier

import (
	"database/sql"
	"errors"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/skeema/internal/fs"
	"github.com/skeema/skeema/internal/tengo"
	"github.com/skeema/skeema/internal/workspace"
)

// Target represents a unit of operation. For each dir that defines at least
// one instance and schema, targets are generated as the cartesian product of
// (instances this dir maps to) x (schemas that this dir maps to on each
// instance).
type Target struct {
	Instance      *tengo.Instance
	Dir           *fs.Dir
	SchemaName    string
	DesiredSchema *workspace.Schema
}

// SchemaFromInstance introspects and returns the instance's version of the
// schema, if it exists.
func (t *Target) SchemaFromInstance() (*tengo.Schema, error) {
	schema, err := t.Instance.Schema(t.SchemaName)
	if err == sql.ErrNoRows {
		err = nil
	}
	schema.StripMatches(t.Dir.IgnorePatterns)
	return schema, err
}

// SchemaFromDir returns the desired schema expressed in the filesystem.
func (t *Target) SchemaFromDir() *tengo.Schema {
	schemaCopy := *t.DesiredSchema.Schema
	schemaCopy.Name = t.SchemaName
	return &schemaCopy
}

func (t *Target) logApplyStart() {
	if t.Dir.Config.GetBool("dry-run") {
		log.Infof("Generating diff of %s %s vs %s%c*.sql", t.Instance, t.SchemaName, t.Dir, os.PathSeparator)
	} else {
		log.Infof("Pushing changes from %s%c*.sql to %s %s", t.Dir, os.PathSeparator, t.Instance, t.SchemaName)
	}
	if len(t.Dir.UnparsedStatements) > 0 {
		log.Warnf("Ignoring %d unsupported or unparseable statements found in this directory's *.sql files; run `skeema lint` for more info", len(t.Dir.UnparsedStatements))
	}
}

func (t *Target) logApplyEnd(result Result) {
	if result.Differences {
		verb := "push"
		if t.Dir.Config.GetBool("dry-run") {
			verb = "diff"
		}
		log.Infof("%s %s: %s complete\n", t.Instance, t.SchemaName, verb)
	} else {
		log.Infof("%s %s: No differences found\n", t.Instance, t.SchemaName)
	}
}

func (t *Target) processSQL(stmts []PlannedStatement, printer Printer) (skipCount int) {
	for i, stmt := range stmts {
		printer.Print(stmt)
		if !t.Dir.Config.GetBool("dry-run") {
			if err := stmt.Execute(); err != nil {
				log.Errorf("Error running SQL statement on %s %s: %s\nFull SQL statement: %s%s", t.Instance, t.SchemaName, err, stmt.Statement(), stmt.ClientState().Delimiter)
				skipped := len(stmts) - i
				skipCount += skipped
				if skipped > 1 {
					log.Warnf("Skipping %d remaining operations for %s %s due to previous error", skipped-1, t.Instance, t.SchemaName)
				}
				return
			}
		}
	}
	if printerFinisher, ok := printer.(Finisher); ok && len(stmts) > 0 {
		printerFinisher.Finish(t)
	}
	return
}

// TargetGroup represents a group of Targets that all have the same Instance.
type TargetGroup []*Target

// TargetsForDir examines dir's configuration, figures out what Target(s) the
// dir maps to, and then recursively descends through dir's subdirectories to
// do the same.
//
// If firstOnly is true, any directory that normally maps to multiple instances
// and/or schemas will only use of the first of each.
//
// Targets are returned as a slice with no guaranteed ordering. Errors are not
// fatal; a count of skipped dirs is returned instead.
func TargetsForDir(dir *fs.Dir, maxDepth int) (targets []*Target, skipCount int) {
	if dir.ParseError != nil {
		log.Errorf("Skipping %s: %s\n", dir.Path, dir.ParseError)
		return nil, 1
	}

	if dir.Config.Changed("host") && dir.HasSchema() {
		var instances []*tengo.Instance
		instances, skipCount = instancesForDir(dir)

		// For each LogicalSchema, obtain a *tengo.Schema representation and then
		// create a Target for each instance x schema combination
		if len(instances) > 0 {
			for n, logicalSchema := range dir.LogicalSchemas {
				thisTargets, thisSkipCount := targetsForLogicalSchema(logicalSchema, dir, instances)
				targets = append(targets, thisTargets...)
				skipCount += thisSkipCount
				if thisSkipCount > 0 {
					// If something went wrong, stop processing any other LogicalSchemas and
					// don't recurse into subdirs, since there's likely something fatally wrong
					// with this dir's configuration
					skipCount += len(dir.LogicalSchemas) - 1 - n
					return
				}
			}
		}
	} else if dir.HasSchema() {
		// If we have a schema defined but no host, display a warning
		log.Warnf("Skipping %s: no host defined for environment %q", dir, dir.Config.Get("environment"))
		if dir.Config.Changed("host-wrapper") {
			log.Warn("The host-wrapper option controls which hosts this directory maps to, but a .skeema file also needs to set the host option to some value (even just a placeholder value) for this command to properly map directories to hosts and schemas.\n")
		} else {
			log.Warnf("This command requires a hostname, which can be specified using the host option in a [%s] section of the .skeema file in this directory, or in a parent directory.\n", dir.Config.Get("environment"))
		}

	} else if dir.OptionFile != nil && dir.OptionFile.SomeSectionHasOption("schema") {
		// If we don't have a schema defined, but we would if some other environment
		// had been selected, display a warning
		log.Warnf("Skipping %s: no schema defined for environment %q\nOther environments do define a schema name. Refer to the .skeema file in this directory for details.\n", dir, dir.Config.Get("environment"))
	}

	subdirs, err := dir.Subdirs()
	if err != nil {
		log.Warnf("Skipping subdirs of %s: %s\n", dir, err)
		skipCount++
		return
	} else if len(subdirs) > 0 && maxDepth < 1 {
		log.Warnf("Skipping subdirs of %s: max depth reached\n", dir)
		skipCount += len(subdirs)
		return
	}

	for _, subdir := range subdirs {
		subTargets, subSkipCount := TargetsForDir(subdir, maxDepth-1)
		targets = append(targets, subTargets...)
		skipCount += subSkipCount
	}
	return
}

func instancesForDir(dir *fs.Dir) (instances []*tengo.Instance, skipCount int) {
	if dir.Config.GetBool("first-only") {
		onlyInstance, err := dir.FirstInstance()
		if onlyInstance == nil && err == nil {
			log.Warnf("Skipping %s: directory maps to an empty list of instances", dir)
			if dir.Config.Changed("host-wrapper") {
				log.Warnf("The host-wrapper option controls which hosts this directory maps to, but the executed script returned no hostnames.\n")
			}
			return nil, 0
		} else if err != nil {
			log.Errorf("Skipping %s: %s\n", dir, err)
			return nil, 1
		}
		// dir.FirstInstance already checks for connectivity and flavor mismatches,
		// so no need to redo that here
		return []*tengo.Instance{onlyInstance}, 0
	}

	rawInstances, err := dir.Instances()
	if err != nil {
		log.Errorf("Skipping %s: %s\n", dir, err)
		return nil, 1
	} else if len(rawInstances) == 0 {
		log.Warnf("Skipping %s: directory maps to an empty list of instances", dir)
		if dir.Config.Changed("host-wrapper") {
			log.Warnf("The host-wrapper option controls which hosts this directory maps to, but the executed script returned no hostnames.\n")
		}
		return nil, 0
	}
	// dir.Instances doesn't pre-check for connectivity problems, so do that now
	for _, inst := range rawInstances {
		if err := dir.ValidateInstance(inst); err != nil {
			log.Errorf("Skipping %s for %s: %s\n", inst, dir, err)
			skipCount++
			continue
		}
		instances = append(instances, inst)
	}
	return
}

func targetsForLogicalSchema(logicalSchema *fs.LogicalSchema, dir *fs.Dir, instances []*tengo.Instance) (targets []*Target, skipCount int) {
	// If there are multiple logical schemas defined in this directory, prohibit
	// mixing configuration styles. Either all CREATEs should be in a single
	// unnamed logical schema (with schema name controlled via .skeema file), OR
	// all CREATEs should have prior USE commands or db name prefixes.
	if logicalSchema.Name == "" && len(dir.LogicalSchemas) > 1 && len(dir.NamedSchemaStatements) > 0 {
		log.Errorf("Skipping %s: some statements reference specific schema names, for example %s line %d.", dir, dir.NamedSchemaStatements[0].File, dir.NamedSchemaStatements[0].LineNo)
		log.Error("When configuring a schema name in .skeema, please omit schema names entirely from *.sql files.\n")
		return nil, len(instances)
	}

	// Confirm all instances have the same lower_case_table_names; mixing isn't
	// supported within a single operation
	if len(instances) > 1 && instances[0].NameCaseMode() != tengo.NameCaseUnknown {
		lctn := instances[0].NameCaseMode()
		for _, other := range instances[1:] {
			if compare := other.NameCaseMode(); compare != tengo.NameCaseUnknown && compare != lctn {
				log.Errorf("Skipping %s: all database servers mapped by the same subdirectory and environment must have the same value for lower_case_table_names.", dir)
				log.Errorf("Instance %s has lower_case_table_names=%d, but instance %s has lower_case_table_names=%d.", instances[0], lctn, other, compare)
				return nil, len(instances)
			}
		}
	}

	// Obtain a *tengo.Schema representation of the dir's *.sql files from a
	// workspace
	opts, err := workspace.OptionsForDir(dir, instances[0])
	if err != nil {
		log.Errorf("Skipping %s: %s\n", dir, err)
		return nil, len(instances)
	}
	wsSchema, err := workspace.ExecLogicalSchema(logicalSchema, opts)
	if err != nil {
		// Since `skeema diff` / `skeema push` is often used in Docker-based CI, add
		// extra text if the error was caused by lack of Docker CLI on PATH
		if errors.Is(err, tengo.ErrNoDockerCLI) {
			err = fmt.Errorf("%w\nSkeema v1.11+ no longer includes a built-in Docker client. If you are using workspace=docker while also running `skeema` itself in a container, be sure to put a Linux `docker` client CLI binary in the container as well.\nFor more information, see https://github.com/skeema/skeema/issues/186#issuecomment-1648483625", err)
		}
		log.Errorf("Skipping %s: %s\n", dir, err)
		return nil, len(instances)
	}
	if len(wsSchema.Failures) > 0 {
		logFailedStatements(dir, wsSchema.Failures)
		return nil, len(instances)
	}

	// Create a Target for each instance x schema combination
	for _, inst := range instances {
		// Obtain the list of schema names configured in .skeema
		schemaNames, err := dir.SchemaNames(inst)
		if err != nil {
			log.Errorf("Skipping %s for %s: %s\n", inst, dir, err)
			skipCount++
			continue
		}

		// If this LogicalSchema has no name, it means use the list from the dir
		// config. Otherwise, use the LogicalSchema's own name, but only if there
		// isn't also a conflicting configuration in the .skeema file.
		if logicalSchema.Name == "" { // blank means use the schema option from dir config
			if len(schemaNames) == 0 {
				log.Warnf("Skipping %s for %s: no schema names returned\n", inst, dir)
			} else if dir.Config.GetBool("first-only") {
				schemaNames = schemaNames[0:1]
			}
		} else {
			// Prohibit conflicting configurations in .skeema + CREATE or USE statements.
			// The only allowed cases are either NO schema name in .skeema, OR a single
			// schema name in .skeema which exactly matches a single schema name used
			// consistently throughout this dir's *.sql files.
			if len(schemaNames) > 0 {
				if len(schemaNames) > 1 || len(dir.LogicalSchemas) > 1 || schemaNames[0] != logicalSchema.Name {
					log.Errorf("Skipping %s: This directory's .skeema file configures a different schema name than its *.sql files.", dir)
					log.Error("When configuring a schema name in .skeema, exclude schema names entirely from *.sql files.\n")
					return nil, len(instances)
				}
			}
			schemaNames = []string{logicalSchema.Name}
		}
		for _, schemaName := range schemaNames {
			t := &Target{
				Instance:      inst,
				Dir:           dir,
				SchemaName:    schemaName,
				DesiredSchema: wsSchema,
			}
			targets = append(targets, t)
		}
	}
	return
}

// TargetGroupsForDir returns a slice of TargetGroups (Target values grouped by
// Instance) for this dir and its subdirs, and count of directories that were
// skipped due to non-fatal errors.
func TargetGroupsForDir(dir *fs.Dir) ([]TargetGroup, int) {
	targets, skipCount := TargetsForDir(dir, 5)
	byInst := make(map[string]TargetGroup)
	for _, t := range targets {
		key := t.Instance.String()
		byInst[key] = append(byInst[key], t)
	}
	groups := []TargetGroup{}
	for _, tg := range byInst {
		groups = append(groups, tg)
	}
	return groups, skipCount
}

func logFailedStatements(dir *fs.Dir, failures []*workspace.StatementError) {
	for _, stmtErr := range failures {
		log.Error(stmtErr.Error())
	}
	noun := "errors"
	if len(failures) == 1 {
		noun = "error"
	}
	log.Warnf("Skipping %s due to %d SQL %s\n", dir, len(failures), noun)
}
