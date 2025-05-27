package mybase

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"unicode"
)

// Section represents a labeled section of an option file. Option values that
// precede any named section are still associated with a Section object, but
// with a Name of "".
type Section struct {
	Name   string
	Values map[string]string  // mapping of option name => value as string
	opts   map[string]*Option // mapping of option name => option definition
}

// File represents a form of ini-style option file. Lines can contain
// [sections], option=value, option without value (usually for bools), or
// comments.
type File struct {
	Dir                  string
	Name                 string
	IgnoreUnknownOptions bool
	sections             []*Section
	sectionIndex         map[string]*Section
	read                 bool
	parsed               bool
	contents             string
	selected             []string
	ignoredOptionNames   map[string]bool
	onlyOptionNames      map[string]bool
}

// NewFile returns a value representing an option file. The arg(s) will be
// joined to create a single path, so it does not matter if the path is provided
// in a way that separates the dir from the base filename or not.
func NewFile(paths ...string) *File {
	pathAndName := filepath.Join(paths...)
	cleanPath, err := filepath.Abs(filepath.Clean(pathAndName))
	if err == nil {
		pathAndName = cleanPath
	}

	defaultSection := &Section{
		Name:   "",
		Values: make(map[string]string),
		opts:   make(map[string]*Option),
	}

	return &File{
		Dir:                filepath.Dir(pathAndName),
		Name:               filepath.Base(pathAndName),
		sections:           []*Section{defaultSection},
		sectionIndex:       map[string]*Section{"": defaultSection},
		ignoredOptionNames: make(map[string]bool),
		onlyOptionNames:    make(map[string]bool),
	}
}

// Exists returns true if the file exists and is visible to the current user.
func (f *File) Exists() bool {
	_, err := os.Stat(f.Path())
	return (err == nil)
}

// Path returns the file's full absolute path with filename.
func (f *File) Path() string {
	return filepath.Join(f.Dir, f.Name)
}

func (f *File) String() string {
	return f.Path()
}

// Write writes out the file's contents to disk. If overwrite=false and the
// file already exists, an error will be returned.
// Note that if overwrite=true and the file already exists, any comments
// and extra whitespace in the file will be lost upon re-writing. All option
// names and values will be normalized in the rewritten file. Any "loose-"
// prefix option names that did not exist will not be written, and any that
// did exist will have their "loose-" prefix stripped. These shortcomings will
// be fixed in a future release.
func (f *File) Write(overwrite bool) error {
	lines := make([]string, 0)
	for n, section := range f.sections {
		if section.Name != "" {
			lines = append(lines, fmt.Sprintf("[%s]", section.Name))
		}

		ks := make([]string, 0, len(section.Values))
		for k := range section.Values {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		for _, k := range ks {
			// Note: section.opts[k] will be nil if the option value came from
			// File.SetOptionValue() and was not previously set! In this case we always
			// treat the opt as stringy, to avoid converting some-int=0 to skip-some-int
			optionIsBoolean := (section.opts[k] != nil && section.opts[k].Type == OptionTypeBool)
			val := section.Values[k]
			if (optionIsBoolean && !BoolValue(val)) || val == "''" { // false-valued boolean, or explicitly-empty-string non-boolean
				lines = append(lines, fmt.Sprintf("skip-%s", k))
			} else if optionIsBoolean || val == "" { // true-valued boolean, or valueless (implying value-optional) non-boolean
				lines = append(lines, k)
			} else { // non-boolean with a value
				lines = append(lines, fmt.Sprintf("%s=%s", k, val))
			}
		}

		// Append a blank line after the section, unless it was the last one, or
		// it was the default section and had no values
		if n < len(f.sections)-1 && (section.Name != "" || len(section.Values) > 0) {
			lines = append(lines, "")
		}
	}

	if len(lines) == 0 {
		log.Printf("Skipping write to %s due to empty configuration", f.Path())
		return nil
	}
	f.contents = fmt.Sprintf("%s\n", strings.Join(lines, "\n"))
	f.read = true
	f.parsed = true

	flag := os.O_WRONLY | os.O_CREATE
	if overwrite {
		flag |= os.O_TRUNC
	} else {
		flag |= os.O_EXCL
	}
	osFile, err := os.OpenFile(f.Path(), flag, 0666)
	if err != nil {
		return err
	}
	n, err := osFile.Write([]byte(f.contents))
	if err == nil && n < len(f.contents) {
		err = io.ErrShortWrite
	}
	if err1 := osFile.Close(); err == nil {
		err = err1
	}
	return err
}

// Read loads the contents of the option file, but does not parse it.
func (f *File) Read() error {
	file, err := os.Open(f.Path())
	if err != nil {
		return err
	}
	defer file.Close()
	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	f.contents = string(bytes)
	f.read = true
	return nil
}

// Parse parses the file contents into a series of Sections. A Config object
// must be supplied so that the list of valid Options is known.
func (f *File) Parse(cfg *Config) error {
	if !f.read {
		if err := f.Read(); err != nil {
			return err
		}
	}

	section := f.sectionIndex[""]

	var lineNumber int
	contents := strings.TrimPrefix(f.contents, "\uFEFF") // strip utf8 BOM if present
	scanner := bufio.NewScanner(strings.NewReader(contents))
	for scanner.Scan() {
		line := scanner.Text()
		lineNumber++

		parsedLine, err := parseLine(line)
		if err != nil {
			return FileParseFormatError{
				Problem:    err.Error(),
				FilePath:   f.Path(),
				LineNumber: lineNumber,
			}
		}

		switch parsedLine.kind {
		case lineTypeSectionHeader:
			section = f.getOrCreateSection(parsedLine.sectionName)
		case lineTypeKeyOnly, lineTypeKeyValue:
			if f.ignoredOptionNames[parsedLine.key] || (len(f.onlyOptionNames) > 0 && !f.onlyOptionNames[parsedLine.key]) {
				continue
			}
			opt := cfg.FindOption(parsedLine.key)
			if opt == nil {
				if parsedLine.isLoose || f.IgnoreUnknownOptions || cfg.LooseFileOptions {
					continue
				} else {
					return OptionNotDefinedError{parsedLine.key, fmt.Sprintf("%s line %d", f.Path(), lineNumber)}
				}
			}
			if parsedLine.kind == lineTypeKeyOnly {
				if opt.RequireValue {
					return OptionMissingValueError{opt.Name, fmt.Sprintf("%s line %d", f.Path(), lineNumber)}
				} else if opt.Type == OptionTypeBool {
					// For booleans, option without value indicates option is being enabled
					parsedLine.value = "1"
				}
			} else if parsedLine.value == "" && opt.Type == OptionTypeString {
				// Convert empty strings into quote-wrapped empty strings, so that callers
				// may differentiate between bare "foo" vs "foo=" if desired, by using
				// Config.GetRaw(). Meanwhile Config.Get and most other getters strip
				// surrounding quotes, so this does not break anything.
				parsedLine.value = "''"
			}
			section.Values[parsedLine.key] = parsedLine.value
			section.opts[parsedLine.key] = opt
		}
	}

	f.parsed = true
	f.selected = []string{""}
	return scanner.Err()
}

// UseSection changes which section(s) of the file are used when calling
// OptionValue. If multiple section names are supplied, multiple sections will
// be checked by OptionValue, with sections listed first taking precedence over
// subsequent ones.
// Note that the default nameless section "" (i.e. lines at the top of the file
// prior to a section header) is automatically appended to the end of the list.
// So this section is always checked, at lowest priority, need not be
// passed to this function.
func (f *File) UseSection(names ...string) error {
	notFound := make([]string, 0)
	already := make(map[string]bool, len(names))

	// This intentionally allocates a new []string for selected. This way, even
	// if there are other shallow copies of f, calling UseSection on one won't
	// affect the others.
	f.selected = make([]string, 0, len(names)+1)

	for _, name := range names {
		if already[name] {
			continue
		}
		already[name] = true
		if f.HasSection(name) {
			f.selected = append(f.selected, name)
		} else {
			notFound = append(notFound, name)
		}
	}
	if !already[""] {
		f.selected = append(names, "")
	}

	if len(notFound) == 0 {
		return nil
	}
	return fmt.Errorf("File %s missing section: %s", f.Path(), strings.Join(notFound, ", "))
}

// HasSection returns true if the file has a section with the supplied name.
func (f *File) HasSection(name string) bool {
	_, ok := f.sectionIndex[name]
	return ok
}

// SectionsWithOption returns a list of section names that set the supplied
// option name.
func (f *File) SectionsWithOption(optionName string) []string {
	result := make([]string, 0, len(f.sections))
	for _, section := range f.sections {
		if _, ok := section.Values[optionName]; ok {
			result = append(result, section.Name)
		}
	}
	return result
}

// SomeSectionHasOption returns true if at least one section sets the supplied
// option name.
func (f *File) SomeSectionHasOption(optionName string) bool {
	return len(f.SectionsWithOption(optionName)) > 0
}

// SectionValues returns a map of option name to raw option string values for
// the supplied section name. The returned map is a copy; modifying it will not
// affect the File.
func (f *File) SectionValues(name string) map[string]string {
	section := f.sectionIndex[name]
	if section == nil {
		return map[string]string{}
	}
	result := make(map[string]string, len(section.Values))
	for k, v := range section.Values {
		result[k] = v
	}
	return result
}

// OptionValue returns the value for the requested option from the option file.
// Only the previously-selected section(s) of the file will be used, or the
// default section "" if no section has been selected via UseSection.
// Panics if the file has not yet been parsed, as this would indicate a bug.
// This is satisfies the OptionValuer interface, allowing Files to be used as
// an option source in Config.
func (f *File) OptionValue(optionName string) (string, bool) {
	if !f.parsed {
		panic(fmt.Errorf("Call to OptionValue(\"%s\") on unparsed file %s", optionName, f.Path()))
	}
	for _, sectionName := range f.selected {
		section := f.sectionIndex[sectionName]
		if section == nil {
			continue
		}
		if value, ok := section.Values[optionName]; ok {
			return value, true
		}
	}
	return "", false
}

// SetOptionValue sets an option value in the named section. This is not
// persisted to the file until Write is called on the File. This is not
// guaranteed to affect any Config that is already using the File as a
// Source.
func (f *File) SetOptionValue(sectionName, optionName, value string) {
	section := f.getOrCreateSection(sectionName)
	section.Values[optionName] = value
}

// UnsetOptionValue removes an option value in the named section. This is not
// persisted to the file until Write is called on the File. This is not
// guaranteed to affect any Config that is already using the File as a
// Source.
func (f *File) UnsetOptionValue(sectionName, optionName string) {
	section := f.getOrCreateSection(sectionName)
	delete(section.Values, optionName)
}

// SameContents returns true if f and other have the same sections and values.
// Ordering, formatting, comments, filename, and directory do not affect the
// results of this comparison. Both files must be parsed by the caller prior
// to calling this method, otherwise this method panics to indicate programmer
// error.
// This method is primarily intended for unit testing purposes.
func (f *File) SameContents(other *File) bool {
	if !f.parsed || !other.parsed {
		panic(errors.New("File.SameContents called on a file that has not yet been parsed"))
	}
	if len(f.sectionIndex) != len(other.sectionIndex) {
		return false
	}
	for name := range f.sectionIndex {
		a := f.sectionIndex[name]
		b, ok := other.sectionIndex[name]
		if !ok || a.Name != b.Name {
			return false
		}
		if !reflect.DeepEqual(a.Values, b.Values) {
			return false
		}
	}
	return true
}

// IgnoreOptions causes the supplied option names to be ignored by a subsequent
// call to Parse. The supplied option names do not need to exist as valid
// options.
// Note that if the file is later re-written, ignored options will be stripped
// from the rewritten version.
// Panics if the file has already been parsed, as this would indicate a bug.
func (f *File) IgnoreOptions(names ...string) {
	if f.parsed {
		panic(errors.New("File.IgnoreOptions called on a file that has already been parsed"))
	}
	for _, name := range names {
		f.ignoredOptionNames[name] = true
		delete(f.onlyOptionNames, name)
	}
}

// LimitOptions causes the subsequent call to Parse to ignore all options other
// than the ones that were explicitly specified in calls to LimitOptions. You
// may call this method multiple times, and the effect is additive.
// This method does not verify the existence of the supplied option names, but
// they should exist as valid options, since they will be processed if
// encountered in the file.
// Note that if the file is later re-written, ignored options will be stripped
// from the rewritten version.
// Panics if the file has already been parsed, as this would indicate a bug.
func (f *File) LimitOptions(names ...string) {
	if f.parsed {
		panic(errors.New("File.IgnoreOptions called on a file that has already been parsed"))
	}
	for _, name := range names {
		f.onlyOptionNames[name] = true
		delete(f.ignoredOptionNames, name)
	}
}

// DeprecationWarnings returns a slice of warning messages for usage of
// deprecated options in any section of the file. This satisfies the
// DeprecationWarner interface.
func (f *File) DeprecationWarnings() []string {
	if !f.parsed {
		panic(fmt.Errorf("Call to DeprecationWarnings() on unparsed file %s", f.Path()))
	}
	var warnings []string
	for _, section := range f.sections {
		for name, opt := range section.opts {
			if opt.Deprecated() {
				warnings = append(warnings, f.Path()+": Option "+name+" is deprecated. "+opt.deprecationDetails)
			}
		}
	}
	return warnings
}

func (f *File) getOrCreateSection(name string) *Section {
	if s, exists := f.sectionIndex[name]; exists {
		return s
	}
	s := &Section{
		Name:   name,
		Values: make(map[string]string),
		opts:   make(map[string]*Option),
	}
	f.sections = append(f.sections, s)
	f.sectionIndex[name] = s
	return s
}

type lineType int

const (
	lineTypeBlank lineType = iota
	lineTypeComment
	lineTypeSectionHeader
	lineTypeKeyOnly
	lineTypeKeyValue
)

type parsedLine struct {
	sectionName string
	key         string
	value       string
	comment     string
	kind        lineType
	isLoose     bool
}

// parseLine parses a file line into its components
func parseLine(line string) (*parsedLine, error) {
	line = strings.TrimLeftFunc(line, unicode.IsSpace)
	result := new(parsedLine)

	if line == "" {
		result.kind = lineTypeBlank
		return result, nil
	}
	if line[0] == ';' || line[0] == '#' {
		result.kind = lineTypeComment
		result.comment = line[1:]
		return result, nil
	}

	if line[0] == '[' {
		endIndex := strings.Index(line, "]")
		hashIndex := strings.Index(line, "#")
		if endIndex == -1 || (hashIndex > -1 && hashIndex < endIndex) {
			return nil, errors.New("unterminated section name")
		}
		if endIndex < len(line)-1 {
			var after string
			if hashIndex > -1 {
				after = line[endIndex+1 : hashIndex]
			} else {
				after = line[endIndex+1:]
			}
			if len(strings.TrimSpace(after)) > 0 {
				return nil, errors.New("extra characters after section name")
			}
		}
		result.kind = lineTypeSectionHeader
		result.sectionName = line[1:endIndex]
		if hashIndex > -1 {
			result.comment = line[hashIndex+1:]
		}
		return result, nil
	}

	// If we get here, it's one of the key/value types
	var inValue, escapeNext bool
	var inQuote rune

	// Parse out any inline comment, being careful to still allow escaped hashes or
	// hashes inside of quoted values
	for n, c := range line {
		if escapeNext {
			escapeNext = false
			continue
		}
		if c == '#' && inQuote == 0 {
			result.comment = line[n+1:]
			line = line[0:n]
			break
		}
		if !inValue {
			switch c {
			case '=':
				inValue = true
			case '\'', '"', '`', '\\':
				return nil, fmt.Errorf("Illegal character %c in option name", c)
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			if c == inQuote {
				inQuote = 0
			} else if inQuote == 0 {
				inQuote = c
			}
		case '\\':
			escapeNext = true
		}
	}

	if inQuote != 0 {
		return nil, errors.New("Quoted value has no terminating quote")
	}
	if escapeNext {
		return nil, errors.New("Value ends in a single backslash")
	}

	var hasValue bool
	result.key, result.value, hasValue, result.isLoose = NormalizeOptionToken(line)
	if hasValue {
		result.kind = lineTypeKeyValue
	} else {
		result.kind = lineTypeKeyOnly
	}
	return result, nil
}

// FileParseFormatError is an error returned when File.Parse encounters a
// problem with the formatting of a file (separate from an unknown option or a
// lack of a required value for an option, which are handled by other types)
type FileParseFormatError struct {
	Problem    string
	FilePath   string
	LineNumber int
}

// Error satisfies golang's error interface.
func (fpf FileParseFormatError) Error() string {
	return fmt.Sprintf("Parse error in %s line %d: %s", fpf.FilePath, fpf.LineNumber, fpf.Problem)
}
