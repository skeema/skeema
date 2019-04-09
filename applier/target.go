package applier

import (
	"strings"

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

		// For each LogicalSchema, obtain a *tengo.Schema representation and then
		// create a Target for each instance x schema combination
		for _, logicalSchema := range dir.LogicalSchemas {
			thisTargets, thisSkipCount := targetsForLogicalSchema(logicalSchema, dir, instances)
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

// checkInstanceFlavor examines the actual flavor of the supplied instance,
// and compares to the directory's configured flavor. If both are valid but
// differ, log a warning. If the instance flavor cannot be detected but the
// directory has a known flavor, override the instance to use the configured
// dir flavor.
func checkInstanceFlavor(instance *tengo.Instance, dir *fs.Dir) {
	instFlavor := instance.Flavor()
	confFlavor := tengo.NewFlavor(dir.Config.Get("flavor"))

	if instFlavor.Known() {
		if confFlavor != tengo.FlavorUnknown && instFlavor != confFlavor {
			log.Warnf("Instance %s actual flavor %s differs from dir %s configured flavor %s", instance, instFlavor, dir, confFlavor)
		}
	} else {
		if confFlavor == tengo.FlavorUnknown {
			log.Warnf("Instance %s flavor cannot be parsed, and dir %s does not specify a flavor override in .skeema", instance, dir)
		} else {
			log.Debugf("Instance %s flavor cannot be parsed; using dir %s configured flavor %s instead", instance, dir, confFlavor)
			instance.SetFlavor(confFlavor)
		}
	}
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
		checkInstanceFlavor(onlyInstance, dir)
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
			checkInstanceFlavor(inst, dir)
			instances = append(instances, inst)
		}
	}
	return
}

func targetsForLogicalSchema(logicalSchema *fs.LogicalSchema, dir *fs.Dir, instances []*tengo.Instance) (targets []*Target, skipCount int) {
	// If dir mapped to no instances, it generates no targets
	if len(instances) == 0 {
		return
	}

	// Obtain a *tengo.Schema representation of the dir's *.sql files from a
	// workspace
	opts, err := workspace.OptionsForDir(dir, instances[0])
	if err != nil {
		log.Warnf("Skipping %s: %s\n", dir, err)
		return nil, len(instances)
	}
	fsSchema, statementErrors, err := workspace.ExecLogicalSchema(logicalSchema, opts)
	if err != nil {
		log.Warnf("Skipping %s: %s\n", dir, err)
		return nil, len(instances)
	}
	for _, stmtErr := range statementErrors {
		log.Error(stmtErr.Error())
		if (strings.Contains(stmtErr.Error(), "Error 1031") || strings.Contains(stmtErr.Error(), "Error 1067")) && !dir.Config.Changed("connect-options") {
			log.Info("This may be caused by Skeema's default usage of strict-mode settings. To disable strict-mode, add this to a .skeema file:")
			log.Info("connect-options=\"innodb_strict_mode=0,sql_mode='ONLY_FULL_GROUP_BY,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'\"\n")
		}
	}
	if len(statementErrors) > 0 {
		noun := "errors"
		if len(statementErrors) == 1 {
			noun = "error"
		}
		log.Warnf("Skipping %s due to %d SQL %s\n", dir, len(statementErrors), noun)
		return nil, len(instances)
	}

	// Create a Target for each instance x schema combination
	for _, inst := range instances {
		var schemaNames []string
		if logicalSchema.Name == "" { // blank means use the schema option from dir config
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
			schemaNames = []string{logicalSchema.Name}
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
