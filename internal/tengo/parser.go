package tengo

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"unicode/utf8"
)

// This file implements a simple partial SQL parser. It intentionally does not
// aim to be a complete parser, and does not use an AST; full SQL parsing is an
// explicit non-goal. Ability to handle invalid SQL is actually a goal. The
// purpose of this parser is just to identify statement types, object types,
// object names, schema name qualifiers, DEFINER clauses, and delimiters.

// Token represents a lexical token in a .sql file.
type Token struct {
	val    string
	typ    TokenType
	offset uint32 // starting position of val inside of Statement.Text
}

// ParseStatements splits the contents of the supplied io.Reader into
// distinct SQL statements. The filePath is descriptive and only used in error
// messages.
//
// Statements preserve their whitespace and delimiters; the return value exactly
// represents the entire input. Some of the returned "statements" may just be
// comments and/or whitespace, since any comments and/or whitespace between SQL
// statements gets split into separate Statement values. Other "statements" are
// actually client commands (USE, DELIMITER).
func ParseStatements(r io.Reader, filePath string) (result []*Statement, err error) {
	p := newParser(r, filePath, ";")
	for {
		stmt, err := p.nextStatement()
		if stmt != nil {
			result = append(result, stmt)
		}
		if err == io.EOF {
			return result, nil
		} else if err != nil {
			return result, err
		}
	}
}

// ParseStatementsInFile opens the file at filePath and then calls
// ParseStatements with it as the reader.
func ParseStatementsInFile(filePath string) (result []*Statement, err error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	return ParseStatements(f, filePath)
}

// ParseStatementsInString uses a strings.Reader to parse statements from the
// supplied string.
func ParseStatementsInString(s string) (result []*Statement, err error) {
	r := strings.NewReader(s)
	return ParseStatements(r, "")
}

// ParseStatementInString returns the first Statement that can be found in its
// input. If the input is an empty string, and/or if an error occurs, then a
// zero-valued Statement will be returned, rather than a nil Statement. Note
// that the zero value of its Type field is StatementTypeUnknown.
// Since leading whitespace and/or comments are considered a separate
// "statement", be aware that this will mask any subsequent "real" statements
// later in the string.
// For situations that require a specific error value, or the ability to detect
// zero or 2+ statements in the input, use ParseStatementsInString instead.
func ParseStatementInString(s string) *Statement {
	statements, err := ParseStatementsInString(s)
	if err == nil && len(statements) > 0 {
		return statements[0]
	}
	return &Statement{}
}

type parser struct {
	lexer *Lexer

	stmt *Statement      // tracking current (not yet completed parsing) statement
	b    strings.Builder // buffer for building text of under-construction statement
	err  error           // only set once an error occurs during scanning (eof, io error, etc)

	defaultDatabase   string
	explicitDelimiter bool // true only if a DELIMITER command has ever been encountered in this input

	filePath   string
	lineNumber int
	colNumber  int
}

type statementProcessor func(p *parser, tokens []Token) (*Statement, error)

var processors map[string]statementProcessor
var createProcessors map[string]statementProcessor

// init here registers the default set of top-level statement processors and
// types of CREATE statement processors.
func init() {
	processors = map[string]statementProcessor{
		"create":    processCreateStatement,
		"use":       processUseCommand,
		"delimiter": processDelimiterCommand,
	}
	createProcessors = map[string]statementProcessor{
		"table":     processCreateTable,
		"function":  processCreateRoutine,
		"procedure": processCreateRoutine,
		"definer":   processCreateWithDefiner,
	}
}

func newParser(r io.Reader, filePath, delimiter string) *parser {
	return &parser{
		lexer:      NewLexer(r, delimiter, 8192),
		filePath:   filePath,
		lineNumber: 1,
		colNumber:  1,
	}
}

// positionAfterBuffer returns the line number and column number corresponding
// to the parser's position immediately after the currently-buffered text.
func (p *parser) positionAfterBuffer() (lineNumber, colNumber int) {
	lineNumber, colNumber = p.lineNumber, p.colNumber
	s := p.b.String()
	pos := strings.IndexByte(s, '\n')
	for pos >= 0 {
		lineNumber++
		colNumber = 1
		s = s[pos+1:]
		pos = strings.IndexByte(s, '\n')
	}
	colNumber += utf8.RuneCountInString(s)
	return
}

func (p *parser) nextStatement() (stmt *Statement, err error) {
	if p.stmt != nil {
		panic(fmt.Errorf("parser.nextStatement: at %s:%d:%d, previous statement not closed properly", p.filePath, p.lineNumber, p.colNumber))
	}
	p.stmt = &Statement{
		File:            p.filePath,
		LineNo:          p.lineNumber,
		CharNo:          p.colNumber,
		DefaultDatabase: p.defaultDatabase,
		Delimiter:       p.lexer.Delimiter(),
	}

	// At beginning of input, check for UTF-8 BOM as a special case. Otherwise
	// scan for first token of statement.
	var t Token
	if p.lineNumber == 1 && p.colNumber == 1 && p.lexer.ScanBOM() {
		// BOM is treated as TokenFiller / StatementTypeNoop. This is the only
		// situation where two StatementTypeNoop "statements" may occur in a row;
		// normally they're combined into a single statement.
		// The BOM noop statement will also be located at "char 0" on the 1st line.
		p.b.WriteString("\uFEFF")
		t = Token{typ: TokenFiller, val: p.b.String()}
		p.stmt.CharNo = 0
		p.colNumber--
	} else {
		t, err = p.nextToken()
	}

	if err != nil {
		return nil, err
	} else if t.typ == TokenFiller || t.typ == TokenDelimiter {
		p.stmt.Type = StatementTypeNoop
		return p.finishStatement(), nil
	}

	var processor statementProcessor
	if t.typ == TokenWord {
		processor = processors[strings.ToLower(string(t.val))]
	}
	if processor == nil {
		// Default processor is used if statement starts with a non-keyword, or with
		// a keyword that this package does not support; in these cases we leave
		// p.stmt.Type at its default of StatementTypeUnknown.
		processor = processUntilDelimiter
	}
	// t is effectively consumed here; we pass a nil token list into the processor
	return processor(p, nil)
}

// nextToken returns the next token in the input stream.
func (p *parser) nextToken() (Token, error) {
	var t Token
	if p.err != nil {
		return t, p.err
	}
	var val []byte
	val, t.typ, p.err = p.lexer.Scan()
	if p.err != nil {
		return t, p.err
	}

	// lexer.Scan won't return an error alongside a non-empty Token, but
	// p.lexer.err will be non-nil immediately. Check for MalformedSQLError
	// *before* processing the token, so that we can annotate the error with
	// position info based on the *start* of the problematic token, for example
	// the start of an unclosed quote or comment.
	if p.lexer.err != nil {
		if mse, ok := p.lexer.err.(*MalformedSQLError); ok {
			mse.filePath = p.filePath
			mse.lineNumber, mse.colNumber = p.positionAfterBuffer()
		}
	}

	t.offset = uint32(p.b.Len())
	p.b.Write(val)
	t.val = p.b.String()[t.offset:]
	return t, nil
}

// nextTokens attempts to grow the supplied tokens list to ensure it is at
// least n tokens in length, unless it already is. This method won't grow a
// list beyond a delimiter token or error, so the result is not guaranteed to
// be n tokens long. The result always excludes TokenFiller tokens. Errors are
// not returned, but may be obtained via p.err if necessary.
// The supplied tokens list may be nil, if no tokens have been buffered by
// caller. If it is non-nil, it must either contain no TokenDelimiter, or
// have its only TokenDelimiter occur at the end of the slice. The intended
// call pattern is to obtain tokens from nextTokens, process some of them, and
// then supply a subslice of any remaining tokens back to the subsequent call to
// nextTokens.
func (p *parser) nextTokens(tokens []Token, n int) []Token {
	for p.err == nil && len(tokens) < n && (len(tokens) == 0 || tokens[len(tokens)-1].typ != TokenDelimiter) {
		t, err := p.nextToken()
		if err == nil && t.typ != TokenFiller {
			tokens = append(tokens, t)
		}
	}
	return tokens
}

// finishStatement marks the current statement as completed, returning it after
// cleaning up some bookkeeping state. finishStatement should generally be
// called after encountering a delimiter token, or after encountering a newline
// which completes a mysql client command which doesn't require a delimiter (USE
// command, DELIMITER command, etc).
func (p *parser) finishStatement() *Statement {
	stmt := p.stmt
	stmt.Text = p.b.String()
	p.lineNumber, p.colNumber = p.positionAfterBuffer()
	p.b.Reset()
	p.stmt = nil
	return stmt
}

// matchNextSequence attempts to find a matching sequence at the start of
// tokens. The match is greedy, meaning the longest matching wantSequence will
// be used. The supplied tokens will be grown as needed, if possible. Supplying
// tokens=nil is allowed. Each wantSequence should be supplied as an all-
// lowercase single-spaced string of token values. Separate lists of matching
// and leftover tokens will be returned; the former will be nil if no match
// occurred.
func (p *parser) matchNextSequence(tokens []Token, wantSequence ...string) (matched []Token, leftovers []Token) {
	if len(wantSequence) == 0 {
		return nil, tokens
	}

	// Pre-process the input to determine the number of tokens in each wanted
	// sequence, also tracking the token count in the longest wanted sequence.
	// Then sort sequences from highest token count to least.
	var maxWantLen int
	sequences := make([]struct {
		val string
		cnt int
	}, len(wantSequence))
	for n, ws := range wantSequence {
		sequences[n].val = ws + " " // trailing space enables prefix matching below
		sequences[n].cnt = strings.Count(ws, " ") + 1
		if sequences[n].cnt > maxWantLen {
			maxWantLen = sequences[n].cnt
		}
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i].cnt > sequences[j].cnt })

	// Attempt to ensure tokens list is long enough to possibly match the longest
	// sequence, and then build a string out of the first tokens. We can then see
	// if any sequence is a prefix of this built string. Since the sequences are
	// sorted in desc order of token length, matching is inherently greedy.
	tokens = p.nextTokens(tokens, maxWantLen)
	var b strings.Builder
	for n := 0; n < maxWantLen && n < len(tokens); n++ {
		b.WriteString(strings.ToLower(tokens[n].val))
		b.WriteByte(' ')
	}
	s := b.String()
	for _, seq := range sequences {
		if strings.HasPrefix(s, seq.val) {
			return tokens[0:seq.cnt], tokens[seq.cnt:]
		}
	}
	return nil, tokens // no match
}

// skipUntilSequence searches for the first occurrence of a supplied sequence.
// It will consume any supplied tokens first, obtaining additional tokens as
// needed. If the sequence is not found, searching stops once a delimiter or
// error occurs. The match is greedy, meaning the longest matching wantSequence
// will be used. Each wantSequence should be supplied as an all-lowercase
// single-spaced string of token values. Separate lists of matching and leftover
// tokens will be returned. If no match occurred, matched will be nil, and
// leftovers will either contain a single delimiter token or be nil.
func (p *parser) skipUntilSequence(tokens []Token, wantSequence ...string) (matched []Token, leftovers []Token) {
	if len(wantSequence) == 0 {
		return nil, tokens
	}

	// Pre-process the input to determine the first token of each wanted sequence,
	// as well as the token count in the longest wanted sequence
	wantFirst := make([]string, len(wantSequence))
	var maxWantLen int
	for n, ws := range wantSequence {
		wantLen := strings.Count(ws, " ") + 1
		if wantLen > maxWantLen {
			maxWantLen = wantLen
		}
		if wantLen == 1 {
			wantFirst[n] = ws
		} else if firstSpace := strings.IndexByte(ws, ' '); firstSpace > -1 {
			wantFirst[n] = ws[0:firstSpace]
		}
	}

	// Attempt to maintain a token list of sufficient size and keep looping until
	// we hit a delimiter or run out of tokens (the latter implying an error/EOF)
	tokens = p.nextTokens(tokens, 5*maxWantLen)
	for len(tokens) > 0 && tokens[0].typ != TokenDelimiter {
		// see if first token in list matches first token of any wantSequence, and
		// track as a candidate match if so
		lowerFirstToken := strings.ToLower(tokens[0].val)
		var candidates []string
		for n, wf := range wantFirst {
			if lowerFirstToken == wf {
				if maxWantLen == 1 { // simplest case, every wantSequence is just one token, so our single-token match is sufficient
					return tokens[0:1], tokens[1:]
				}
				candidates = append(candidates, wantSequence[n])
			}
		}

		// check candidates for full match; if none, advance list
		matched, tokens = p.matchNextSequence(tokens, candidates...)
		if matched != nil {
			return matched, tokens
		}
		tokens = p.nextTokens(tokens[1:], maxWantLen)
	}
	return nil, tokens
}

func (p *parser) parseObjectNameClause(tokens []Token) (leftovers []Token) {
	// Ensure we have enough tokens
	tokens = p.nextTokens(tokens, 3)
	if len(tokens) < 1 {
		return nil
	}

	// See if we have a schema name qualifier
	if len(tokens) >= 3 && tokens[1].typ == TokenSymbol && tokens[1].val[0] == '.' {
		schemaName, schemaOK := getNameFromToken(tokens[0])
		objectName, objectOK := getNameFromToken(tokens[2])
		if schemaOK && objectOK {
			p.stmt.ObjectQualifier, p.stmt.ObjectName = schemaName, objectName
			p.stmt.nameClause = p.b.String()[tokens[0].offset : int(tokens[2].offset)+len(tokens[2].val)]
			return tokens[3:]
		}
		return tokens // can't parse
	}

	objectName, objectOK := getNameFromToken(tokens[0])
	if objectOK {
		p.stmt.ObjectName = objectName
		p.stmt.nameClause = p.b.String()[tokens[0].offset : int(tokens[0].offset)+len(tokens[0].val)]
		return tokens[1:]
	}

	return tokens // can't parse
}

func getNameFromToken(t Token) (name string, ok bool) {
	if t.typ == TokenIdent {
		name = stripBackticks(t.val)
	} else if t.typ == TokenWord {
		name = t.val
	} else if t.typ == TokenString && t.val[0] == '"' { // ansi_quotes sql mode?
		name = stripAnyQuote(t.val)
	}
	return name, name != ""
}

// processUntilDelimiter scans and discards tokens until a delimiter is found
// or an error occurs. It does not modify p.stmt.Type. The supplied tokens may
// be nil (if no tokens have been buffered by caller), or a non-nil slice that
// either contains no TokenDelimiter or has its only TokenDelimiter at the end
// of the slice. (This is compatible with how nextTokens operates).
func processUntilDelimiter(p *parser, tokens []Token) (stmt *Statement, err error) {
	// Check if we've already buffered a list of tokens ending in a delimiter.
	// If not, scan next token in a tight loop until we hit delimiter or error.
	if len(tokens) == 0 || tokens[len(tokens)-1].typ != TokenDelimiter {
		var t Token
		for err == nil && t.typ != TokenDelimiter {
			t, err = p.nextToken()
		}
	}
	return p.finishStatement(), err
}

func processUseCommand(p *parser, _ []Token) (stmt *Statement, err error) {
	var (
		dbBuilder        strings.Builder
		ignoreRestOfLine bool
		t                Token
	)

	// USE command may be terminated by just a newline, OR by normal delimiter
	p.lexer.commandMode = true

	// Typically, the first token will be TokenFiller, followed by either
	// TokenWord or tokenIdent. However, unquoted database names may also contain
	// symbols in the USE command (since the mysql client has different parsing
	// rules than the server), and the line may also contain extra args after
	// whitespace which are just ignored by the mysql client.
	for {
		t, err = p.nextToken()
		if err != nil || t.typ == TokenDelimiter {
			break
		} else if t.typ == TokenFiller {
			ignoreRestOfLine = (dbBuilder.Len() > 0)
		} else if ignoreRestOfLine {
			continue
		} else if t.typ == TokenIdent {
			dbBuilder.WriteString(stripBackticks(t.val))
			ignoreRestOfLine = true
		} else {
			dbBuilder.WriteString(t.val)
		}
	}
	if newDefaultDB := dbBuilder.String(); newDefaultDB != "" {
		p.stmt.Type = StatementTypeCommand
		p.defaultDatabase = newDefaultDB
	}
	return p.finishStatement(), err
}

func processDelimiterCommand(p *parser, _ []Token) (stmt *Statement, err error) {
	var (
		delimBuilder     strings.Builder
		ignoreRestOfLine bool
		t                Token
	)

	// DELIMITER command is terminated by a newline
	p.lexer.commandMode = true

	// DELIMITER command itself cannot have any other delimiter, so temporarily
	// change the current delimiter to a null zero to prevent lexer from
	// incorrectly emitting TokenDelimiter when changing delimiter from e.g. ";"
	// to ";;". Also manipulate it in the under-construction Statement, to prevent
	// Statement.SplitTextBody() from misbehaving.
	oldDelim := p.lexer.Delimiter()
	p.lexer.ChangeDelimiter("\000")
	p.stmt.Delimiter = "\000"

	// Typically, the first token will be TokenFiller, followed by a mix of one or
	// more TokenSymbol (each individual operator rune is considered a separate
	// token!) and/or TokenWord (since TokenSymbol excludes some runes like '$').
	// However, the delimiter may optionally be quoted, and the line may contain
	// extra args after whitespace which are just ignored by the mysql client.
	for {
		t, err = p.nextToken()
		if err != nil {
			break
		} else if t.typ == TokenDelimiter { // "\n" or "\r\n" via commandMode
			break
		} else if t.typ == TokenFiller {
			ignoreRestOfLine = (delimBuilder.Len() > 0)
		} else if ignoreRestOfLine {
			continue
		} else if t.typ == TokenString || t.typ == TokenIdent { // delimiter supplied as quote-wrapped string
			delimBuilder.WriteString(stripAnyQuote(t.val))
			ignoreRestOfLine = true
		} else {
			delimBuilder.WriteString(t.val)
		}
	}
	newDelim := delimBuilder.String()
	if newDelim == "" { // line failed to specify the new delimiter!
		newDelim = oldDelim
	}
	p.stmt.Type = StatementTypeCommand
	p.lexer.ChangeDelimiter(newDelim)
	p.explicitDelimiter = true // disable permissive parsing of compound stored program bodies in input lacking DELIMITER
	return p.finishStatement(), err
}

func processCreateStatement(p *parser, tokens []Token) (stmt *Statement, err error) {
	tokens = p.nextTokens(tokens, 20)
	if len(tokens) < 2 {
		return p.finishStatement(), p.err
	}

	var processor statementProcessor
	if tokens[0].typ == TokenWord {
		processor = createProcessors[strings.ToLower(tokens[0].val)]
	}
	if processor == nil {
		processor = processUntilDelimiter
	}
	return processor(p, tokens)
}

func processCreateTable(p *parser, tokens []Token) (*Statement, error) {
	// Skip past the TABLE token, and ignore the optional IF NOT EXIST
	// clause
	_, tokens = p.matchNextSequence(tokens[1:], "if not exists")

	// Attempt to parse object name; only set statement and object types if
	// successful
	tokens = p.parseObjectNameClause(tokens)
	if p.stmt.ObjectName != "" {
		p.stmt.Type = StatementTypeCreate
		p.stmt.ObjectType = ObjectTypeTable
	}

	matched, tokens := p.skipUntilSequence(tokens, "select")
	if matched != nil {
		p.err = MalformedSQLError{
			str:        "Statements of the form CREATE TABLE...SELECT are not supported",
			filePath:   p.filePath,
			lineNumber: p.lineNumber,
			colNumber:  p.colNumber,
		}
	}

	return processUntilDelimiter(p, tokens)
}

func processCreateRoutine(p *parser, tokens []Token) (*Statement, error) {
	matched, tokens := p.matchNextSequence(tokens, "procedure", "function")
	if matched == nil {
		return processUntilDelimiter(p, tokens) // cannot parse, unexpected token
	}

	// Ignore the optional IF NOT EXIST clause
	_, tokens = p.matchNextSequence(tokens, "if not exists")

	// Attempt to parse object name; only set statement and object types if
	// successful
	tokens = p.parseObjectNameClause(tokens)
	if p.stmt.ObjectName != "" {
		p.stmt.Type = StatementTypeCreate
		p.stmt.ObjectType = ObjectType(strings.ToLower(matched[0].val))
	}

	return processStoredProgram(p, tokens)
}

func processCreateWithDefiner(p *parser, tokens []Token) (*Statement, error) {
	// ensure we have enough additional tokens to match the longest definer clause
	// format, plus one additional token to know which processor to call next
	tokens = p.nextTokens(tokens, 6)

	if len(tokens) < 4 {
		return processUntilDelimiter(p, tokens) // cannot parse, minimal definer clause is 3 tokens + 1 next token
	}

	matched, tokens := p.matchNextSequence(tokens, "definer =")
	if len(matched) != 2 {
		return processUntilDelimiter(p, tokens) // cannot parse, unexpected tokens
	}

	// Consume the tokens with the definer value: one of CURRENT_USER, CURRENT_USER(), or user@host
	if matched, tokens = p.matchNextSequence(tokens, "current_user", "current_user ( )"); matched == nil {
		if len(tokens) < 4 || tokens[1].typ != TokenSymbol || tokens[1].val != "@" {
			return processUntilDelimiter(p, tokens) // cannot parse, expected to find user @ host
		}
		tokens = tokens[3:]
	}

	// Now delegate to the appropriate processor for the type of create statement
	// indicated by the next token
	var processor statementProcessor
	if len(tokens) > 0 && tokens[0].typ == TokenWord {
		processor = createProcessors[strings.ToLower(tokens[0].val)]
	}
	if processor == nil {
		processor = processUntilDelimiter
	}
	return processor(p, tokens)
}

// processStoredProgram parses the definition of a stored program (proc/func/
// trigger/event) after the initial part of the CREATE statement. This may
// include args (proc/func), return value (func), and body of the statement,
// which may or may not be a compound statement (BEGIN block). If no explicit
// DELIMITER command has already been encountered in the input, this parsing is
// permissive of compound statements and won't treat semicolons as delimiters,
// meaning the entire remaining input is considered a single statement.
func processStoredProgram(p *parser, tokens []Token) (stmt *Statement, err error) {
	var n int
	var t Token
	var compound bool
	for {
		// If we've already obtained some tokens, use those; otherwise get another one
		if n < len(tokens) {
			t = tokens[n]
			n++
		} else {
			t, err = p.nextToken()
			if err != nil {
				break
			}
		}

		// Stop looping on the delimiter token, unless the input didn't have an
		// explicit DELIMITER command and we've already seen a BEGIN keyword, in which
		// case we treat the entire input as a single compound statement.
		// Since BEGIN isn't a reserved word, it is possible this will misdetect
		// single-statement procs/funcs that happen to have an arg called "begin", but
		// that situation is rare and it's typically harmless to set compound=true.
		// The only pathological case is when there's no DELIMITER command *and* a
		// single-statement proc/func has an arg called "begin" *and* other statements
		// follow it in the input. We don't account for that case since it means the
		// input wasn't generated by Skeema in the first place.
		if t.typ == TokenDelimiter && (p.explicitDelimiter || !compound) {
			break
		}
		if !compound && t.typ == TokenWord && strings.ToLower(t.val) == "begin" {
			compound = true
		}
	}
	stmt = p.finishStatement()
	stmt.Compound = compound
	return
}

func stripBackticks(input string) string {
	if len(input) < 2 || input[0] != '`' || input[len(input)-1] != '`' {
		return input
	}
	input = input[1 : len(input)-1]
	return strings.Replace(input, "``", "`", -1)
}

func stripAnyQuote(input string) string {
	if len(input) < 2 || input[0] != input[len(input)-1] {
		return input
	}
	if input[0] == '`' {
		return stripBackticks(input)
	} else if input[0] != '"' && input[0] != '\'' {
		return input
	}
	quoteStr := input[0:1]
	input = input[1 : len(input)-1]
	input = strings.Replace(input, strings.Repeat(quoteStr, 2), quoteStr, -1)
	return strings.Replace(input, fmt.Sprintf("\\%s", quoteStr), quoteStr, -1)
}
