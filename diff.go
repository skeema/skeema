package main

func init() {
	long := `Compares the schemas on database instance(s) to the corresponding
filesystem representation of them. The output is a series of DDL commands that,
if run on the instance, would cause the instances' schemas to now match the
ones in the filesystem.`

	Commands["diff"] = Command{
		Name:    "diff",
		Short:   "Compare a DB instance's schemas and tables to the filesystem",
		Long:    long,
		Flags:   nil,
		Handler: DiffCommand,
	}

	// TODO: don't want host etc cli options here... maybe they shouldn't be global...
}

func DiffCommand(cfg Config) {
	dir := NewSkeemaDir(cfg.GlobalFlags.Path)

	// reminder: this should recurse into subdirs too
}
