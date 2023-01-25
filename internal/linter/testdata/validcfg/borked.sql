# This would normally generate an Error annotation via AnnotateStatementErrors,
# with RuleName of "sql-syntax"
CREATE TABLE borked1 (
	id int,
	lol I dunno,
	just make me a good table pls);

# Expect this to go in Result.DebugLogs due to ignore-table in .skeema
CREATE TABLE _borked2 (same here ok cool thanks);

# This would normally generate an Error annotation via AnnotateStatementErrors,
# with RuleName of "sql-1072" (error 1072 is for index referring to a col that
# does not exist)
CREATE TABLE borked3 (
	id int,
	PRIMARY KEY (doesntexist)
);


# linter module does not create Annotations for unparsed statements; callers
# handle this separately, so this won't generate an annotation
CALL my_proc(123456);
