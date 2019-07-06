// Package dumper handles writing SQL statements, obtained from objects in a
// live schema, to files in a directory. It can be used to do an initial dump,
// to update a previous dump to reflect changes in a schema, or to reformat
// statements to match canonical formats.
package dumper

import (
	"fmt"
	"regexp"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/skeema/fs"
	"github.com/skeema/tengo"
)

// Options controls dumper behavior.
type Options struct {
	IncludeAutoInc bool                     // if false, strip AUTO_INCREMENT clauses from CREATE TABLE
	CountOnly      bool                     // if true, skip writing files, just report count of rewrites
	IgnoreTable    *regexp.Regexp           // skip tables with names matching this regex
	skipKeys       map[tengo.ObjectKey]bool // skip objects with true values
	onlyKeys       map[tengo.ObjectKey]bool // if map is non-nil, only format objects with true values
}

// OnlyKeys specifies a list of tengo.ObjectKeys that the dump should
// operate on. (Objects with keys NOT in this list will be skipped.)
// Repeated calls to this method add to the existing whitelist.
func (opts *Options) OnlyKeys(keys []tengo.ObjectKey) {
	if opts.onlyKeys == nil {
		opts.onlyKeys = make(map[tengo.ObjectKey]bool, len(keys))
	}
	for _, key := range keys {
		opts.onlyKeys[key] = true
	}
}

// IgnoreKeys specifies a list of tengo.ObjectKeys that the dump should
// ignore. Repeated calls to this method add to the existing blacklist.
// If the same key was supplied to both OnlyKeys and IgnoreKeys, the latter
// takes precedence, meaning the object will be skipped.
func (opts *Options) IgnoreKeys(keys []tengo.ObjectKey) {
	if opts.skipKeys == nil {
		opts.skipKeys = make(map[tengo.ObjectKey]bool, len(keys))
	}
	for _, key := range keys {
		opts.skipKeys[key] = true
	}
}

// shouldIgnore returns true if the option configuration indicates the supplied
// tengo.ObjectKey should be ignored.
func (opts *Options) shouldIgnore(key tengo.ObjectKey) bool {
	if opts.skipKeys[key] {
		return true
	}
	if key.Type == tengo.ObjectTypeTable && opts.IgnoreTable != nil && opts.IgnoreTable.MatchString(key.Name) {
		return true
	}
	if opts.onlyKeys != nil && !opts.onlyKeys[key] {
		return true
	}
	return false
}

// DumpSchema updates the *.sql files in dir to match the creation statements
// in schema. Any preexisting creation statements in the dir will be updated to
// match the canonical format from the live schema. Objects that no longer exist
// in the live schema will have their statements removed. A count of modified
// statements is returned, along with any fatal write error. If opts.CountOnly
// is true, no actual filesystem writes occur, but a count is still returned.
func DumpSchema(schema *tengo.Schema, dir *fs.Dir, opts Options) (count int, err error) {
	// TODO: handle dirs that contain multiple logical schemas by name
	var logicalSchema *fs.LogicalSchema
	if len(dir.LogicalSchemas) > 0 {
		logicalSchema = dir.LogicalSchemas[0]
	} else {
		logicalSchema = &fs.LogicalSchema{}
	}

	filesToRewrite := make(map[*fs.TokenizedSQLFile]bool)
	schemaObjects := schema.ObjectDefinitions()
	for key, stmt := range logicalSchema.Creates {
		if opts.shouldIgnore(key) {
			continue
		}
		if canonicalCreate, existsInSchema := schemaObjects[key]; existsInSchema {
			// Include or strip auto_increment clause. (Note that if fs representation
			// explicitly had an autoinc value > 1, we keep and update it regardless.)
			if key.Type == tengo.ObjectTypeTable && !opts.IncludeAutoInc {
				if _, fsAutoInc := tengo.ParseCreateAutoInc(stmt.Text); fsAutoInc <= 1 {
					canonicalCreate, _ = tengo.ParseCreateAutoInc(canonicalCreate)
				}
			}
			if !fs.CanParse(canonicalCreate) {
				log.Errorf("%s is unexpectedly not able to be parsed by Skeema -- please file a bug at https://github.com/skeema/skeema/issues/new", key)
				continue
			}
			fsCreate, fsDelimiter := stmt.SplitTextBody()
			if canonicalCreate == fsCreate {
				continue // statement already present with correct formatting
			}
			count++
			if !opts.CountOnly {
				stmt.Text = fmt.Sprintf("%s%s", canonicalCreate, fsDelimiter)
				filesToRewrite[stmt.FromFile] = true
			}
		} else {
			count++
			if !opts.CountOnly {
				stmt.Remove()
				filesToRewrite[stmt.FromFile] = true
			}
		}
	}

	// Do the appropriate rewrites of files tracked above. Note that if
	// opts.CountOnly is true, filesToRewrite will be empty, so no need to
	// re-check.
	for file := range filesToRewrite {
		if bytesWritten, err := file.Rewrite(); err != nil {
			return count, err
		} else if bytesWritten == 0 {
			log.Infof("Deleted %s -- no longer exists", file)
		} else {
			log.Infof("Wrote %s (%d bytes) -- updated definition", file, bytesWritten)
		}
	}

	// Objects that exist in schema, but have no corresponding create statement
	// in fs yet: write new files, or append if filename already in use
	for key, canonicalCreate := range schemaObjects {
		if logicalSchema.Creates[key] != nil || opts.shouldIgnore(key) {
			continue
		}
		count++
		if opts.CountOnly {
			continue
		}
		if key.Type == tengo.ObjectTypeTable && !opts.IncludeAutoInc {
			canonicalCreate, _ = tengo.ParseCreateAutoInc(canonicalCreate)
		}
		if !fs.CanParse(canonicalCreate) {
			log.Errorf("%s is unexpectedly not able to be parsed by Skeema -- please file a bug at https://github.com/skeema/skeema/issues/new", key)
			continue
		}
		canonicalCreate = fs.AddDelimiter(canonicalCreate)
		filePath := fs.PathForObject(dir.Path, key.Name)
		if bytesWritten, wasNew, err := fs.AppendToFile(filePath, canonicalCreate); err != nil {
			return count, err
		} else if wasNew {
			log.Infof("Wrote %s (%d bytes) -- new %s", filePath, bytesWritten, key.Type)
		} else {
			log.Infof("Wrote %s (%d bytes) -- appended new %s", filePath, bytesWritten, key.Type)
		}
	}

	return count, nil
}
