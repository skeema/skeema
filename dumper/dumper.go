// Package dumper handles writing SQL statements, obtained from objects in a
// live schema, to files in a directory. It can be used to do an initial dump,
// to update a previous dump to reflect changes in a schema, or to reformat
// statements to match canonical formats.
package dumper

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/skeema/fs"
	"github.com/skeema/tengo"
)

type statement struct {
	canonicalCreate  string
	filesystemCreate string
	filesystemDelim  string
	fsStatement      *fs.Statement
}

// DumpSchema updates the *.sql files in dir to match the creation statements
// in schema. Any preexisting creation statements in the dir will be updated to
// match the canonical format from the live schema. Objects that no longer exist
// in the live schema will have their statements removed. A count of modified
// statements is returned, along with any fatal write error. If opts.CountOnly
// is true, no actual filesystem writes occur, but a count is still returned.
func DumpSchema(schema *tengo.Schema, dir *fs.Dir, opts Options) (count int, err error) {
	filesToRewrite := make(map[*fs.TokenizedSQLFile]bool)
	for key, s := range getStatementMap(schema, dir, opts) {
		if opts.shouldIgnore(key) || s.canonicalCreate == s.filesystemCreate {
			continue
		}

		count++
		if s.fsStatement != nil {
			filesToRewrite[s.fsStatement.FromFile] = true
		}
		if opts.CountOnly {
			continue
		}

		if s.fsStatement == nil { // exists in live db schema but not yet in filesystem
			contents := fs.AddDelimiter(s.canonicalCreate)
			filePath := fs.PathForObject(dir.Path, key.Name)
			if err := appendToFile(filePath, contents); err != nil {
				return count, err
			}
		} else if s.canonicalCreate == "" { // already exists in filesystem, but does not exist in live db schema
			s.fsStatement.Remove()
		} else { // exists in live db schema AND filesystem, but needs reformat/update
			s.fsStatement.Text = fmt.Sprintf("%s%s", s.canonicalCreate, s.filesystemDelim)
		}
	}

	// Do the appropriate rewrites of files tracked above, if requested
	for file := range filesToRewrite {
		if opts.CountOnly {
			log.Infof("File %s requires formatting changes", file)
		} else if err := rewriteSQLFile(file); err != nil {
			return count, err
		}
	}

	return count, nil
}

// getStatementMap builds a mapping of all object keys relevant to this dir,
// regardless of whether they're only in filesystem, only in the live db schema,
// or both.
func getStatementMap(schema *tengo.Schema, dir *fs.Dir, opts Options) map[tengo.ObjectKey]statement {
	statementMap := make(map[tengo.ObjectKey]statement)

	// TODO: handle dirs that contain multiple logical schemas by name
	var logicalSchema *fs.LogicalSchema
	if len(dir.LogicalSchemas) > 0 {
		logicalSchema = dir.LogicalSchemas[0]
	} else {
		logicalSchema = &fs.LogicalSchema{}
	}
	for key, stmt := range logicalSchema.Creates {
		fsCreate, fsDelimiter := stmt.SplitTextBody()
		statementMap[key] = statement{
			filesystemCreate: fsCreate,
			filesystemDelim:  fsDelimiter,
			fsStatement:      stmt,
		}
	}

	schemaObjects := schema.ObjectDefinitions()
	for key, canonicalCreate := range schemaObjects {
		s := statementMap[key] // not a pointer, zero value fine
		s.canonicalCreate = canonicalCreate

		// Include or strip auto_increment clause. (Note that if fs representation
		// already exists and explicitly had an autoinc value > 1, we keep and update
		// it regardless.)
		if key.Type == tengo.ObjectTypeTable && !opts.IncludeAutoInc {
			if _, fsAutoInc := tengo.ParseCreateAutoInc(s.filesystemCreate); fsAutoInc <= 1 {
				s.canonicalCreate, _ = tengo.ParseCreateAutoInc(s.canonicalCreate)
			}
		}

		// If requested, adjust the canonical create to add the partitioning clause
		// from the filesystem create.
		if opts.RetainPartitioning && key.Type == tengo.ObjectTypeTable && s.fsStatement != nil {
			dbCreateBase, dbCreatePart := tengo.ParseCreatePartitioning(s.canonicalCreate)
			_, fsCreatePart := tengo.ParseCreatePartitioning(s.filesystemCreate)
			if dbCreatePart == "" && fsCreatePart != "" {
				s.canonicalCreate = fmt.Sprintf("%s%s", dbCreateBase, fsCreatePart)
			}
		}

		if ok, err := fs.CanParse(s.canonicalCreate); ok {
			statementMap[key] = s
		} else {
			log.Errorf("%s is unexpectedly not able to be parsed by Skeema\nPlease file an issue report at https://github.com/skeema/skeema/issues/new with this information:\nError value=%v", key, err)
			log.Error("Unfortunately this error is fatal and prevents Skeema from being usable in your environment until this is resolved.")
			return nil
		}
	}

	return statementMap
}

// appendToFile appends contents to filePath.
func appendToFile(filePath, contents string) error {
	if bytesWritten, wasNew, err := fs.AppendToFile(filePath, contents); err != nil {
		return err
	} else if wasNew {
		log.Infof("Created %s (%d bytes)", filePath, bytesWritten)
	} else {
		log.Infof("Wrote %s (%d bytes) -- appended new object", filePath, bytesWritten)
	}
	return nil
}

// rewriteSQLFile rewrites a TokenizedSQLFile.
func rewriteSQLFile(file *fs.TokenizedSQLFile) error {
	if bytesWritten, err := file.Rewrite(); err != nil {
		return err
	} else if bytesWritten == 0 {
		log.Infof("Deleted %s", file)
	} else {
		log.Infof("Wrote %s (%d bytes)", file, bytesWritten)
	}
	return nil
}
