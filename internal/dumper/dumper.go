// Package dumper handles writing SQL statements, obtained from objects in a
// live schema, to files in a directory. It can be used to do an initial dump,
// to update a previous dump to reflect changes in a schema, or to reformat
// statements to match canonical formats.
package dumper

import (
	"errors"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/skeema/internal/fs"
	"github.com/skeema/skeema/internal/tengo"
)

// DumpSchema updates the *.sql files in dir to match the creation statements
// in schema. Any preexisting creation statements in the dir will be updated to
// match the canonical format from the live schema. Objects that no longer exist
// in the live schema will have their statements removed. A count of modified
// files is returned, along with any fatal write error. If opts.CountOnly is
// true, no actual filesystem writes occur, but a file count is still returned.
func DumpSchema(schema *tengo.Schema, dir *fs.Dir, opts Options) (int, error) {
	// Ensure that this dir does not reference any schemas by name, either via
	// USE commands or CREATEs with schema name qualifiers
	if len(dir.NamedSchemaStatements) > 0 {
		if len(dir.NamedSchemaStatements) == 1 {
			log.Warnf("This directory contains a statement referencing a specific schema name at %s line %d.", dir.NamedSchemaStatements[0].File, dir.NamedSchemaStatements[0].LineNo)
		} else {
			log.Warnf("This directory contains %d statements referencing specific schema names, for example %s line %d.", len(dir.NamedSchemaStatements), dir.NamedSchemaStatements[0].File, dir.NamedSchemaStatements[0].LineNo)
		}
		log.Warn("Most Skeema commands do not support USE statements or schema-prefixed table names yet.")
		log.Warn("Please configure schema names only in .skeema files.")
		return 0, errors.New("unsupported format of .sql files")
	}

	filesWithDiffs := modifiedFiles(schema, dir, opts)
	for n, file := range filesWithDiffs {
		if opts.CountOnly {
			log.Infof("File %s requires formatting changes", file.FilePath)
			file.Dirty = false // since we marked it as dirty artificially / without actually changing anything
			continue
		}
		exists, _ := file.Exists()
		if bytesWritten, err := file.Write(); err != nil {
			return n, err
		} else if bytesWritten == 0 {
			log.Infof("Deleted %s", file.FilePath)
		} else if exists {
			log.Infof("Wrote %s (%d bytes)", file.FilePath, bytesWritten)
		} else {
			log.Infof("Created %s (%d bytes)", file.FilePath, bytesWritten)
		}
	}
	return len(filesWithDiffs), nil
}

// modifiedFiles determines what SQLFile and Statement changes are needed to
// complete the requested dump operation, and returns the affected SQLFiles.
// If opts.CountOnly is false, the SQLFile and Statement changes will be made
// in-place to the in-memory values in dir, but are not actually persisted to
// the filesystem by this function.
func modifiedFiles(schema *tengo.Schema, dir *fs.Dir, opts Options) []*fs.SQLFile {
	// TODO: handle dirs that contain multiple logical schemas by name
	var logicalSchema *fs.LogicalSchema
	if len(dir.LogicalSchemas) > 0 {
		logicalSchema = dir.LogicalSchemas[0]
	} else {
		logicalSchema = fs.NewLogicalSchema()
	}

	dbObjects := schema.Objects()
	for key, object := range dbObjects {
		if opts.shouldIgnore(object) {
			continue
		}
		canonicalCreate := object.Def()
		var fsCreate string
		stmt := logicalSchema.Creates[key]
		if stmt != nil {
			fsCreate, _ = stmt.SplitTextBody()
		}

		// Include or strip auto_increment clause. (Note that if fs representation
		// already exists and explicitly had an autoinc value > 1, we keep and update
		// it regardless.)
		if key.Type == tengo.ObjectTypeTable && !opts.IncludeAutoInc {
			if _, fsAutoInc := tengo.ParseCreateAutoInc(fsCreate); fsAutoInc <= 1 {
				canonicalCreate, _ = tengo.ParseCreateAutoInc(canonicalCreate)
			}
		}

		// If requested, adjust the canonical create to add the partitioning clause
		// from the filesystem create, or remove it
		if key.Type == tengo.ObjectTypeTable && opts.Partitioning != tengo.PartitioningPermissive {
			dbCreateBase, _ := tengo.ParseCreatePartitioning(canonicalCreate)
			if opts.Partitioning == tengo.PartitioningKeep && fsCreate != "" {
				_, fsCreatePart := tengo.ParseCreatePartitioning(fsCreate)
				canonicalCreate = dbCreateBase + fsCreatePart
			} else if opts.Partitioning == tengo.PartitioningRemove {
				canonicalCreate = dbCreateBase
			}
		}

		if tengo.ParseStatementInString(canonicalCreate).ObjectKey() != key {
			log.Errorf("%s is unexpectedly not able to be parsed by Skeema\nPlease file an issue report at https://github.com/skeema/skeema/issues with the problematic statement, redacting sensitive portions if necessary:\n%s", key, canonicalCreate)
			log.Error("Unfortunately this error is fatal and prevents Skeema from being usable in your environment until this is resolved.")
			return nil
		}

		if stmt == nil {
			// We didn't have a Statement from the fs, so append a new one, or just mark
			// the file as dirty if doing CountOnly.
			sqlFile := dir.FileFor(object)
			if opts.CountOnly {
				sqlFile.Dirty = true
			} else {
				sqlFile.AddCreateStatement(key, canonicalCreate)
			}
		} else if fsCreate != canonicalCreate {
			// Statement came from the fs and we need to update it, or just mark its
			// file as dirty if doing CountOnly
			sqlFile := dir.FileFor(stmt)
			if opts.CountOnly {
				sqlFile.Dirty = true
			} else {
				sqlFile.EditStatementText(stmt, canonicalCreate)
			}
		}
	}

	// Handle create statements that are in FS but do not exist in DB
	for key, stmt := range logicalSchema.Creates {
		if _, inDB := dbObjects[key]; !inDB && !opts.shouldIgnore(key) {
			sqlFile := dir.FileFor(stmt)
			if opts.CountOnly {
				sqlFile.Dirty = true
			} else {
				sqlFile.RemoveStatement(stmt)
			}
		}
	}

	return dir.DirtyFiles()
}
