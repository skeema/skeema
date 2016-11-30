package mycli

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"unicode"
)

// Section represents a labeled section of an option file. Option values that
// precede any named section are still associated with a Section object, but
// with a Name of "".
type Section struct {
	Name   string
	Values map[string]string
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
}

// NewFile returns a value representing an option file. The arg(s) will be
// joined to create a single path, so it does not matter if the path is provided
// in a way that separates the dir from the base filename or not.
func NewFile(paths ...string) *File {
	pathAndName := path.Join(paths...)
	cleanPath, err := filepath.Abs(filepath.Clean(pathAndName))
	if err == nil {
		pathAndName = cleanPath
	}

	defaultSection := &Section{
		Name:   "",
		Values: make(map[string]string),
	}

	return &File{
		Dir:          path.Dir(pathAndName),
		Name:         path.Base(pathAndName),
		sections:     []*Section{defaultSection},
		sectionIndex: map[string]*Section{"": defaultSection},
	}
}

// Exists returns true if the file exists and is visible to the current user.
func (f *File) Exists() bool {
	_, err := os.Stat(f.Path())
	return (err == nil)
}

// Path returns the file's full absolute path with filename.
func (f *File) Path() string {
	return path.Join(f.Dir, f.Name)
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
		for k, v := range section.Values {
			lines = append(lines, fmt.Sprintf("%s=%s", k, v))
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
	scanner := bufio.NewScanner(strings.NewReader(f.contents))
	for scanner.Scan() {
		line := scanner.Text()
		lineNumber++

		parsedLine, err := parseLine(line)
		if err != nil {
			return fmt.Errorf("Parse error in %s line %d: %s", f.Path(), lineNumber, err)
		}

		switch parsedLine.kind {
		case lineTypeSectionHeader:
			section = f.getOrCreateSection(parsedLine.sectionName)
		case lineTypeKeyOnly, lineTypeKeyValue:
			opt := cfg.FindOption(parsedLine.key)
			if opt == nil {
				if parsedLine.isLoose || f.IgnoreUnknownOptions {
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
			}
			section.Values[parsedLine.key] = parsedLine.value
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
// persisted to the file until Write is called on the File.
// If the caller plans to subsequently read configuration values from this
// same File object, it is the caller's responsibility to normalize the
// optionName and value prior to calling this method, and call MarkDirty() on
// any relevant Configs. These shortcomings will be fixed in a future release.
func (f *File) SetOptionValue(sectionName, optionName, value string) {
	section := f.getOrCreateSection(sectionName)
	section.Values[optionName] = value
}

func (f *File) getOrCreateSection(name string) *Section {
	if s, exists := f.sectionIndex[name]; exists {
		return s
	}
	s := &Section{
		Name:   name,
		Values: make(map[string]string),
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
		result.comment = line[1:len(line)]
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
				after = line[endIndex+1 : len(line)]
			}
			if len(strings.TrimSpace(after)) > 0 {
				return nil, errors.New("extra characters after section name")
			}
		}
		result.kind = lineTypeSectionHeader
		result.sectionName = line[1:endIndex]
		if hashIndex > -1 {
			result.comment = line[hashIndex+1 : len(line)]
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
			result.comment = line[n+1 : len(line)]
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
