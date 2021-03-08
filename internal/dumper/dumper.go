// Package dumper handles writing SQL statements, obtained from objects in a
// live schema, to files in a directory. It can be used to do an initial dump,
// to update a previous dump to reflect changes in a schema, or to reformat
// statements to match canonical formats.
package dumper

import (
	"errors"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/skeema/internal/fs"
	"github.com/skeema/tengo"
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
	if namedSchemaStmts := dir.NamedSchemaStatements(); len(namedSchemaStmts) > 0 {
		if len(namedSchemaStmts) == 1 {
			log.Warnf("This directory contains a statement referencing a specific schema name at %s line %d.", namedSchemaStmts[0].File, namedSchemaStmts[0].LineNo)
		} else {
			log.Warnf("This directory contains %d statements referencing specific schema names, for example %s line %d.", len(namedSchemaStmts), namedSchemaStmts[0].File, namedSchemaStmts[0].LineNo)
		}
		log.Warn("Most Skeema commands do not support USE statements or schema-prefixed table names yet.")
		log.Warn("Please configure schema names only in .skeema files.")
		return 0, errors.New("unsupported format of .sql files")
	}

	filesWithDiffs := modifiedFiles(schema, dir, opts)
	for n, file := range filesWithDiffs {
		if opts.CountOnly {
			log.Infof("File %s requires formatting changes", file)
		} else if err := rewriteSQLFile(file); err != nil {
			return n, err
		}
	}
	return len(filesWithDiffs), nil
}

// rewriteSQLFile rewrites a TokenizedSQLFile.
func rewriteSQLFile(file *fs.TokenizedSQLFile) error {
	exists, _ := file.Exists()
	if bytesWritten, err := file.Rewrite(); err != nil {
		return err
	} else if bytesWritten == 0 {
		log.Infof("Deleted %s", file)
	} else if exists {
		log.Infof("Wrote %s (%d bytes)", file, bytesWritten)
	} else {
		log.Infof("Created %s (%d bytes)", file, bytesWritten)
	}
	return nil
}

// modifiedFiles returns TokenizedSQLFile pointers that require re-writing due
// to at least one statement in the file being added, modified, or removed as
// a result of the dump operation. The directory's parsed values are modified
// in-place by this function, but nothing is written to the filesystem yet.
func modifiedFiles(schema *tengo.Schema, dir *fs.Dir, opts Options) []*fs.TokenizedSQLFile {
	fileMap := make(map[string]*fs.TokenizedSQLFile) // filePath string -> tokenized file
	fileDirty := make(map[string]bool)               // filePath string -> bool

	// TODO: handle dirs that contain multiple logical schemas by name
	var logicalSchema *fs.LogicalSchema
	if len(dir.LogicalSchemas) > 0 {
		logicalSchema = dir.LogicalSchemas[0]
	} else {
		logicalSchema = &fs.LogicalSchema{}
	}
	keySeen := make(map[tengo.ObjectKey]bool, len(logicalSchema.Creates))
	for key, stmt := range logicalSchema.Creates {
		keySeen[key] = true
		if filePath := stmt.FromFile.String(); fileMap[filePath] == nil {
			fileMap[filePath] = stmt.FromFile
		}
	}

	for key, canonicalCreate := range schema.ObjectDefinitions() {
		if opts.shouldIgnore(key) {
			continue
		}
		var fsCreate, fsDelimiter string
		stmt := logicalSchema.Creates[key]
		if stmt != nil {
			fsCreate, fsDelimiter = stmt.SplitTextBody()
			delete(keySeen, key)
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
				canonicalCreate = fmt.Sprintf("%s%s", dbCreateBase, fsCreatePart)
			} else if opts.Partitioning == tengo.PartitioningRemove {
				canonicalCreate = dbCreateBase
			}
		}

		if !verifyCanParse(key, canonicalCreate) {
			return nil
		}

		// If fs and db creates match, nothing to update
		canonicalCreate = AddDelimiter(key, canonicalCreate, fsDelimiter)
		if stmt != nil && canonicalCreate == stmt.Text {
			continue
		}

		var filePath string
		if stmt == nil {
			filePath = fs.PathForObject(dir.Path, key.Name)
		} else {
			filePath = stmt.FromFile.Path()
		}
		fileDirty[filePath] = true
		if fileMap[filePath] == nil {
			fileMap[filePath] = &fs.TokenizedSQLFile{
				SQLFile: fs.SQLFile{
					Dir:      dir.Path,
					FileName: fs.FileNameForObject(key.Name),
				},
			}
		}

		if opts.CountOnly {
			continue // Don't mutate stmt if CountOnly
		}

		// Append the statement if it doesn't exist in fs yet; otherwise update it.
		// We "cheat" by potentially omitting some fs fields and potentially including
		// DELIMITER wrappers in a single Statement.Text, but this still works fine
		// for rewriting the file later.
		if stmt == nil {
			f := fileMap[filePath]
			stmt = &fs.Statement{
				Type:       fs.StatementTypeCreate,
				ObjectType: key.Type,
				ObjectName: key.Name,
				FromFile:   f,
			}
			f.Statements = append(f.Statements, stmt)
		}
		stmt.Text = canonicalCreate
	}

	// Remaining keys in keySeen do not exist in db, so remove them
	for key := range keySeen {
		if !opts.shouldIgnore(key) {
			stmt := logicalSchema.Creates[key]
			if !opts.CountOnly {
				stmt.Remove()
			}
			fileDirty[stmt.FromFile.String()] = true
		}
	}

	var result []*fs.TokenizedSQLFile
	for filePath := range fileDirty {
		result = append(result, fileMap[filePath])
	}
	return result
}

// AddDelimiter takes the supplied string and appends a delimiter to the end.
// If the supplied string is a multi-statement routine, delimiter commands will
// be prepended and appended to the string appropriately.
func AddDelimiter(key tengo.ObjectKey, statementBody, oldEnding string) string {
	if oldEnding == "" {
		oldEnding = ";\n"
	}
	if fs.NeedSpecialDelimiter(key, statementBody) {
		trimmedOldEnding := strings.TrimRight(oldEnding, "\n\r\t ")
		if trimmedOldEnding == ";" || trimmedOldEnding == "" {
			return fmt.Sprintf("DELIMITER //\n%s//\nDELIMITER ;\n", statementBody)
		}
	}
	return fmt.Sprintf("%s%s", statementBody, oldEnding)
}

func verifyCanParse(key tengo.ObjectKey, statementBody string) bool {
	ok, err := fs.CanParse(statementBody)
	if !ok {
		log.Errorf("%s is unexpectedly not able to be parsed by Skeema\nPlease file an issue report at https://github.com/skeema/skeema/issues with this information:\nError value=%v", key, err)
		log.Error("Unfortunately this error is fatal and prevents Skeema from being usable in your environment until this is resolved.")
	}
	return ok
}
