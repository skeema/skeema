package applier

import (
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/skeema/fs"
	"github.com/skeema/skeema/workspace"
	"github.com/skeema/tengo"
)

// Target represents a unit of operation. For each dir that defines at least
// one instance and schema, targets are generated as the cartesian product of
// (instances this dir maps to) x (schemas that this dir maps to on each
// instance).
type Target struct {
	Instance           *tengo.Instance
	Dir                *fs.Dir
	SchemaFromInstance *tengo.Schema
	SchemaFromDir      *tengo.Schema
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
	if dir.Config.Changed("host") && dir.HasSchema() {
		var instances []*tengo.Instance
		instances, skipCount = instancesForDir(dir)

		// For each IdealSchema, obtain a *tengo.Schema representation and then create
		// a Target for each instance x schema combination
		for _, idealSchema := range dir.IdealSchemas {
			thisTargets, thisSkipCount := targetsForIdealSchema(idealSchema, dir, instances)
			targets = append(targets, thisTargets...)
			skipCount += thisSkipCount
		}
	} else if dir.HasSchema() {
		// If we have a schema defined but no host, display a warning
		log.Warnf("Skipping %s: no host defined for environment \"%s\"\n", dir, dir.Config.Get("environment"))
	} else if dir.OptionFile != nil && dir.OptionFile.SomeSectionHasOption("schema") {
		// If we don't have a schema defined, but we would if some other environment
		// had been selected, display a warning
		log.Warnf("Skipping %s: no schema defined for environment \"%s\"\n", dir, dir.Config.Get("environment"))
	}

	subdirs, badSubdirCount, err := dir.Subdirs()
	skipCount += badSubdirCount
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
			log.Warnf("Skipping %s: dir maps to an empty list of instances\n", dir)
			return nil, 0
		} else if err != nil {
			log.Warnf("Skipping %s: %s\n", dir, err)
			return nil, 1
		}
		// dir.FirstInstance already checks for connectivity, so no need to redo that here
		return []*tengo.Instance{onlyInstance}, 0
	}

	rawInstances, err := dir.Instances()
	if len(rawInstances) == 0 {
		log.Warnf("Skipping %s: dir maps to an empty list of instances\n", dir)
		return nil, 0
	} else if err != nil {
		log.Warnf("Skipping %s: %s\n", dir, err)
		return nil, 1
	}
	// dir.Instances doesn't pre-check for connectivity problems, so do that now
	for _, inst := range rawInstances {
		if ok, err := inst.CanConnect(); !ok {
			log.Warnf("Skipping %s for %s: %s", inst, dir, err)
			skipCount++
		} else {
			instances = append(instances, inst)
		}
	}
	return
}

func targetsForIdealSchema(idealSchema *fs.IdealSchema, dir *fs.Dir, instances []*tengo.Instance) (targets []*Target, skipCount int) {
	// If dir mapped to no instances, it generates no targets
	if len(instances) == 0 {
		return
	}

	// Obtain a *tengo.Schema representation of the dir's *.sql files from a
	// workspace
	opts := workspace.Options{
		Type:            workspace.TypeTempSchema,
		CleanupAction:   workspace.CleanupActionDrop,
		Instance:        instances[0],
		SchemaName:      dir.Config.Get("temp-schema"),
		LockWaitTimeout: 30 * time.Second,
	}
	if dir.Config.GetBool("reuse-temp-schema") {
		opts.CleanupAction = workspace.CleanupActionNone
	}
	fsSchema, statementErrors, err := workspace.MaterializeIdealSchema(idealSchema, opts)
	if err != nil {
		log.Warnf("Skipping %s: %s\n", dir, err)
		return nil, len(instances)
	}
	for _, stmtErr := range statementErrors {
		log.Error(stmtErr.Error())
	}
	if len(statementErrors) > 0 {
		noun := "errors"
		if len(statementErrors) == 1 {
			noun = "error"
		}
		log.Warnf("Skipping %s due to %d SQL %s", dir, len(statementErrors), noun)
		return nil, len(instances)
	}

	// Create a Target for each instance x schema combination
	for _, inst := range instances {
		var schemaNames []string
		if idealSchema.Name == "" { // blank means use the schema option from dir config
			schemaNames, err = dir.SchemaNames(inst)
			if err != nil {
				log.Warnf("Skipping %s for %s: %s", inst, dir, err)
				skipCount++
				continue
			}
			if len(schemaNames) > 1 && dir.Config.GetBool("first-only") {
				schemaNames = schemaNames[0:1]
			}
		} else {
			schemaNames = []string{idealSchema.Name}
		}
		schemasByName, err := inst.SchemasByName(schemaNames...)
		if err != nil {
			log.Warnf("Skipping %s for %s: %s", inst, dir, err)
			skipCount++
			continue
		}

		for _, schemaName := range schemaNames {
			schemaCopy := *fsSchema
			schemaCopy.Name = schemaName
			t := &Target{
				Instance:           inst,
				Dir:                dir,
				SchemaFromInstance: schemasByName[schemaName], // this may be nil if schema doesn't exist yet; callers handle that
				SchemaFromDir:      &schemaCopy,
			}
			targets = append(targets, t)
		}
	}
	return
}

// TargetGroupChanForDir returns a channel for obtaining TargetGroups for this
// dir and its subdirs, and count of directories that were skipped due to non-
// fatal errors.
func TargetGroupChanForDir(dir *fs.Dir) (<-chan TargetGroup, int) {
	targets, skipCount := TargetsForDir(dir, 5)
	groups := make(chan TargetGroup)
	go func() {
		byInst := make(map[string]TargetGroup)
		for _, t := range targets {
			key := t.Instance.String()
			byInst[key] = append(byInst[key], t)
		}
		for _, tg := range byInst {
			groups <- tg
		}
		close(groups)
	}()
	return groups, skipCount
}
