package dumper

import (
	"github.com/skeema/skeema/internal/tengo"
)

// Options controls dumper behavior.
type Options struct {
	IncludeAutoInc bool                     // if false, strip AUTO_INCREMENT clauses from CREATE TABLE
	Partitioning   tengo.PartitioningMode   // PartitioningKeep: retain previous FS partitioning clause; PartitioningRemove: strip partitioning clause
	CountOnly      bool                     // if true, skip writing files, just report count of rewrites
	skipKeys       map[tengo.ObjectKey]bool // skip objects with true values
	onlyKeys       map[tengo.ObjectKey]bool // if map is non-nil, only format objects with true values
}

// OnlyKeys specifies a list of tengo.ObjectKeys that the dump should
// operate on. (Objects with keys NOT in this list will be skipped.)
// Repeated calls to this method add to the existing allowlist.
func (opts *Options) OnlyKeys(keys []tengo.ObjectKey) {
	if opts.onlyKeys == nil {
		opts.onlyKeys = make(map[tengo.ObjectKey]bool, len(keys))
	}
	for _, key := range keys {
		opts.onlyKeys[key] = true
	}
}

// IgnoreKeys specifies a list of tengo.ObjectKeys that the dump should
// ignore. Repeated calls to this method add to the existing list of ignored
// keys. If the same key was supplied to both OnlyKeys and IgnoreKeys, the
// latter takes precedence, meaning the object will be skipped.
func (opts *Options) IgnoreKeys(keys []tengo.ObjectKey) {
	if opts.skipKeys == nil {
		opts.skipKeys = make(map[tengo.ObjectKey]bool, len(keys))
	}
	for _, key := range keys {
		opts.skipKeys[key] = true
	}
}

// shouldIgnore returns true if the option configuration indicates the supplied
// object should be ignored.
func (opts *Options) shouldIgnore(keyer tengo.ObjectKeyer) bool {
	key := keyer.ObjectKey()
	if opts.skipKeys[key] {
		return true
	}
	if opts.onlyKeys != nil && !opts.onlyKeys[key] {
		return true
	}
	return false
}
