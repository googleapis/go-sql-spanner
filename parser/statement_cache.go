package parser

type statementsCacheEntry struct {
	sql             string
	params          []string
	info            *StatementInfo
	parsedStatement ParsedStatement
}
