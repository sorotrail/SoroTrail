package store

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// MigrationRuleID identifies a live-database schema migration safety rule.
type MigrationRuleID string

const (
	// RuleConcurrentIndexRequired flags CREATE [UNIQUE] INDEX statements on
	// existing tables that omit CONCURRENTLY. Standard index builds acquire a
	// SHARE lock that blocks all INSERT/UPDATE/DELETE traffic for the duration
	// of the table scan.
	RuleConcurrentIndexRequired MigrationRuleID = "concurrent-index-required"

	// RuleConcurrentIndexNoTx flags CREATE/DROP INDEX CONCURRENTLY statements
	// in migration files that either lack the `-- sorotrail:no-transaction`
	// directive or wrap statements in an explicit BEGIN/COMMIT block.
	RuleConcurrentIndexNoTx MigrationRuleID = "concurrent-index-no-tx"

	// RuleNoDropColumn flags ALTER TABLE ... DROP COLUMN in .up.sql migrations
	// unless the migration carries `-- sorotrail:allow-contract-drop-column:`
	// documenting that the column was retired in an earlier release as Stage 5
	// of an expand-and-contract rollout.
	RuleNoDropColumn MigrationRuleID = "no-drop-column"

	// RuleNoRenameColumnOrTable flags ALTER TABLE ... RENAME COLUMN and
	// ALTER TABLE ... RENAME TO in .up.sql migrations, which break live
	// instances running the previous release during a rolling deploy.
	RuleNoRenameColumnOrTable MigrationRuleID = "no-rename-column-or-table"

	// RuleNoAlterColumnType flags ALTER TABLE ... ALTER COLUMN ... TYPE in
	// .up.sql migrations, which rewrites the table under ACCESS EXCLUSIVE lock.
	RuleNoAlterColumnType MigrationRuleID = "no-alter-column-type"

	// RuleNotNullNeedsDefault flags ALTER TABLE ... ADD COLUMN ... NOT NULL
	// without a DEFAULT clause, as well as ALTER TABLE ... ALTER COLUMN ...
	// SET NOT NULL without a preceding NOT VALID + VALIDATE CONSTRAINT check.
	RuleNotNullNeedsDefault MigrationRuleID = "not-null-needs-default"

	// RuleNoUnboundedDataBackfill flags unbounded UPDATE, DELETE, or
	// INSERT INTO ... SELECT backfill statements inside .up.sql DDL migrations.
	// Data backfills on live tables must run in bounded, resumable batches via
	// RunBatchedMigration.
	RuleNoUnboundedDataBackfill MigrationRuleID = "no-unbounded-data-backfill"
)

// DirectiveNoTransaction is the header comment directive that opts a migration
// file out of implicit multi-statement transaction wrapping so statements like
// CREATE INDEX CONCURRENTLY and DROP INDEX CONCURRENTLY can execute safely.
const DirectiveNoTransaction = "-- sorotrail:no-transaction"

// DirectiveAllowContractDropColumn is the header comment directive that
// explicitly marks an ALTER TABLE ... DROP COLUMN migration as Stage 5
// (Contract) of an expand-and-contract rollout after all readers/writers have
// stopped referencing the column in a prior release.
const DirectiveAllowContractDropColumn = "-- sorotrail:allow-contract-drop-column:"

// DirectiveAllowPartitionedParentIndex is the header comment directive that
// permits a metadata-only CREATE INDEX on a partitioned parent table (where
// PostgreSQL forbids CREATE INDEX CONCURRENTLY directly on the parent).
const DirectiveAllowPartitionedParentIndex = "-- sorotrail:allow-partitioned-parent-index:"

// MigrationViolation describes a single live-database migration rule violation.
type MigrationViolation struct {
	File      string
	Line      int
	Rule      MigrationRuleID
	Statement string
	Reason    string
}

func (v MigrationViolation) Error() string {
	return fmt.Sprintf("%s:%d [%s]: %s (%s)", v.File, v.Line, v.Rule, v.Reason, v.Statement)
}

// MigrationException documents a historical pre-#1228 migration violation that
// is grandfathered in so existing versioned migrations remain immutable while
// all new migrations are strictly gated.
type MigrationException struct {
	File   string
	Rule   MigrationRuleID
	Reason string
}

// HistoricalMigrationExceptions lists the audited historical exceptions in
// migrations 0001..0032 that predate the live-database migration gate (#1228).
// Do not add new migrations here; use CONCURRENTLY with
// `-- sorotrail:no-transaction` or the expand-and-contract pattern instead.
var HistoricalMigrationExceptions = []MigrationException{
	{
		File:   "0003_topic_position_indexes.up.sql",
		Rule:   RuleConcurrentIndexRequired,
		Reason: "Pre-partitioning historical migration; superseded by 0007_partition_events.",
	},
	{
		File:   "0004_add_created_at_index.up.sql",
		Rule:   RuleConcurrentIndexRequired,
		Reason: "Pre-partitioning historical migration; superseded by 0007_partition_events.",
	},
	{
		File:   "0007_partition_events.up.sql",
		Rule:   RuleNoRenameColumnOrTable,
		Reason: "Historical one-time cutover from unpartitioned events to partitioned events table.",
	},
	{
		File:   "0007_partition_events.up.sql",
		Rule:   RuleNoUnboundedDataBackfill,
		Reason: "Historical one-time bulk copy from events_legacy into partitioned events table.",
	},
	{
		File:   "0009_add_contract_id_created_at_index.up.sql",
		Rule:   RuleConcurrentIndexRequired,
		Reason: "Targets partitioned parent table events where PostgreSQL forbids CREATE INDEX CONCURRENTLY directly on the parent.",
	},
	{
		File:   "0015_add_tx_hash_index.up.sql",
		Rule:   RuleConcurrentIndexRequired,
		Reason: "Targets partitioned parent table events where PostgreSQL forbids CREATE INDEX CONCURRENTLY directly on the parent.",
	},
	{
		File:   "0016_multi_tenancy.up.sql",
		Rule:   RuleConcurrentIndexRequired,
		Reason: "Historical multi-tenancy migration adding idx_subscriptions_tenant alongside new tenant tables.",
	},
	{
		File:   "0019_partial_index_successful_calls.up.sql",
		Rule:   RuleConcurrentIndexRequired,
		Reason: "Targets partitioned parent table events where PostgreSQL forbids CREATE INDEX CONCURRENTLY directly on the parent.",
	},
	{
		File:   "0021_network_column.up.sql",
		Rule:   RuleConcurrentIndexRequired,
		Reason: "Targets partitioned parent table events where PostgreSQL forbids CREATE INDEX CONCURRENTLY directly on the parent.",
	},
	{
		File:   "0021_network_column.up.sql",
		Rule:   RuleNoDropColumn,
		Reason: "Drops legacy singleton id column on 1-row audit_state table when re-keying by network.",
	},
	{
		File:   "0022_event_quick_filter_indexes.up.sql",
		Rule:   RuleConcurrentIndexRequired,
		Reason: "Targets partitioned parent table events where PostgreSQL forbids CREATE INDEX CONCURRENTLY directly on the parent.",
	},
	{
		File:   "0023_network_ingestion_state.up.sql",
		Rule:   RuleConcurrentIndexRequired,
		Reason: "Targets partitioned parent table events where PostgreSQL forbids CREATE INDEX CONCURRENTLY directly on the parent.",
	},
	{
		File:   "0023_network_ingestion_state.up.sql",
		Rule:   RuleNoDropColumn,
		Reason: "Drops legacy singleton id column on 1-row ingestion_state table when re-keying by network.",
	},
	{
		File:   "0023_network_ingestion_state.up.sql",
		Rule:   RuleNotNullNeedsDefault,
		Reason: "Sets network NOT NULL on 1-row singleton ingestion_state table immediately after populating default.",
	},
	{
		File:   "0023_network_ingestion_state.up.sql",
		Rule:   RuleNoUnboundedDataBackfill,
		Reason: "Updates 1-row singleton ingestion_state table before primary-key swap.",
	},
	{
		File:   "0027_watched_contracts_network.up.sql",
		Rule:   RuleConcurrentIndexRequired,
		Reason: "Historical index update on small operator/tenant watch-list table.",
	},
	{
		File:   "0027_watched_contracts_network.up.sql",
		Rule:   RuleNoUnboundedDataBackfill,
		Reason: "Historical ctid self-join deduplication on small operator watched_contracts table.",
	},
	{
		File:   "0028_tenant_watched_contracts_network_pkey.up.sql",
		Rule:   RuleNoUnboundedDataBackfill,
		Reason: "Historical ctid self-join deduplication on small tenant_watched_contracts table.",
	},
}

var (
	reCreateTableStmt  = regexp.MustCompile(`(?is)^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_.]+)`)
	reCreateIndexStmt  = regexp.MustCompile(`(?is)^\s*CREATE\s+(?:UNIQUE\s+)?INDEX\s+(CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?(?:CONCURRENTLY\s+)?[a-zA-Z0-9_.]+\s+ON\s+(?:ONLY\s+)?([a-zA-Z0-9_.]+)`)
	reDropIndexConc    = regexp.MustCompile(`(?is)^\s*DROP\s+INDEX\s+CONCURRENTLY\b`)
	reBeginCommitStmt  = regexp.MustCompile(`(?is)^\s*(BEGIN|START\s+TRANSACTION|COMMIT|END)\b`)
	reAlterTableHead   = regexp.MustCompile(`(?is)^\s*ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?([a-zA-Z0-9_.]+)\s+(.+)$`)
	reDropColumnClause = regexp.MustCompile(`(?is)\bDROP\s+COLUMN\b`)
	reRenameClause     = regexp.MustCompile(`(?is)\bRENAME\s+(?:COLUMN\s+[a-zA-Z0-9_.]+\s+TO|TO)\b`)
	reAlterTypeClause  = regexp.MustCompile(`(?is)\bALTER\s+(?:COLUMN\s+)?[a-zA-Z0-9_.]+\s+(?:SET\s+DATA\s+)?TYPE\b`)
	reSetNotNullClause = regexp.MustCompile(`(?is)\bALTER\s+(?:COLUMN\s+)?([a-zA-Z0-9_.]+)\s+SET\s+NOT\s+NULL\b`)
	reAddColumnClause  = regexp.MustCompile(`(?is)\bADD\s+(?:COLUMN\s+)?(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_]+)\s+([^,;]+)`)
	reValidateCheck    = regexp.MustCompile(`(?is)\bVALIDATE\s+CONSTRAINT\s+([a-zA-Z0-9_.]+)\b`)
	reNotValidCheck    = regexp.MustCompile(`(?is)\bADD\s+CONSTRAINT\s+([a-zA-Z0-9_.]+)\s+CHECK\s*\(\s*([a-zA-Z0-9_.]+)\s+IS\s+NOT\s+NULL\s*\)\s+NOT\s+VALID\b`)
	reUpdateStmt       = regexp.MustCompile(`(?is)^\s*UPDATE\s+([a-zA-Z0-9_.]+)\b`)
	reDeleteStmt       = regexp.MustCompile(`(?is)^\s*DELETE\s+FROM\s+([a-zA-Z0-9_.]+)\b`)
	reInsertSelectStmt = regexp.MustCompile(`(?is)^\s*INSERT\s+INTO\s+([a-zA-Z0-9_.]+)\b.*\bSELECT\b(?:.*\bFROM\s+([a-zA-Z0-9_.]+)\b)?`)
)

// HasNoTransactionDirective reports whether the SQL migration content carries
// the `-- sorotrail:no-transaction` header directive.
func HasNoTransactionDirective(sqlContent string) bool {
	return hasHeaderDirective(sqlContent, DirectiveNoTransaction)
}

func hasHeaderDirective(sqlContent, directive string) bool {
	for _, line := range strings.Split(sqlContent, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if !strings.HasPrefix(trimmed, "--") {
			break
		}
		if strings.HasPrefix(strings.ToLower(trimmed), strings.ToLower(directive)) {
			return true
		}
	}
	return false
}

// sqlStatement is a single top-level SQL statement with its starting 1-based line number.
type sqlStatement struct {
	line int
	text string
}

// parseTopLevelSQLStatements strips SQL comments and blanks out dollar-quoted
// PL/pgSQL bodies while preserving line numbers, then splits top-level
// statements on semicolons.
func parseTopLevelSQLStatements(sqlContent string) []sqlStatement {
	cleaned := stripCommentsAndDollarBodies(sqlContent)
	lines := strings.Split(cleaned, "\n")

	var stmts []sqlStatement
	var buf strings.Builder
	stmtStartLine := 0

	for idx, line := range lines {
		lineNum := idx + 1
		remain := line
		for {
			semi := strings.IndexByte(remain, ';')
			if semi == -1 {
				if strings.TrimSpace(remain) != "" && stmtStartLine == 0 {
					stmtStartLine = lineNum
				}
				buf.WriteString(remain)
				buf.WriteByte('\n')
				break
			}
			part := remain[:semi]
			if strings.TrimSpace(part) != "" && stmtStartLine == 0 {
				stmtStartLine = lineNum
			}
			buf.WriteString(part)
			stmtText := strings.TrimSpace(buf.String())
			if stmtText != "" {
				if stmtStartLine == 0 {
					stmtStartLine = lineNum
				}
				stmts = append(stmts, sqlStatement{
					line: stmtStartLine,
					text: collapseWhitespace(stmtText),
				})
			}
			buf.Reset()
			stmtStartLine = 0
			remain = remain[semi+1:]
		}
	}

	if tail := strings.TrimSpace(buf.String()); tail != "" {
		if stmtStartLine == 0 {
			stmtStartLine = 1
		}
		stmts = append(stmts, sqlStatement{
			line: stmtStartLine,
			text: collapseWhitespace(tail),
		})
	}
	return stmts
}

// stripCommentsAndDollarBodies replaces `--` comments, `/* ... */` block
// comments, and `$$ ... $$` dollar-quoted bodies with spaces while preserving
// newlines so statement line numbers remain exact. Single-quoted string
// literals are preserved (with any `;` inside single quotes replaced by `_` so
// semicolon splitting never breaks mid-string).
func stripCommentsAndDollarBodies(src string) string {
	out := make([]byte, len(src))
	for i := range out {
		out[i] = ' '
	}

	n := len(src)
	i := 0
	for i < n {
		if src[i] == '\n' {
			out[i] = '\n'
			i++
			continue
		}
		// Line comment: -- ...
		if i+1 < n && src[i] == '-' && src[i+1] == '-' {
			for i < n && src[i] != '\n' {
				i++
			}
			continue
		}
		// Block comment: /* ... */
		if i+1 < n && src[i] == '/' && src[i+1] == '*' {
			i += 2
			for i < n {
				if src[i] == '\n' {
					out[i] = '\n'
					i++
					continue
				}
				if i+1 < n && src[i] == '*' && src[i+1] == '/' {
					i += 2
					break
				}
				i++
			}
			continue
		}
		// Single-quoted literal: '...'
		if src[i] == '\'' {
			out[i] = '\''
			i++
			for i < n {
				if src[i] == '\n' {
					out[i] = '\n'
					i++
					continue
				}
				if src[i] == '\'' {
					out[i] = '\''
					i++
					if i < n && src[i] == '\'' {
						out[i] = '\''
						i++
						continue
					}
					break
				}
				if src[i] == ';' {
					out[i] = '_'
				} else {
					out[i] = src[i]
				}
				i++
			}
			continue
		}
		// Dollar-quoted body: $$ ... $$ or $tag$ ... $tag$
		if src[i] == '$' {
			if tag := matchDollarTag(src[i:]); tag != "" {
				i += len(tag)
				for i < n {
					if src[i] == '\n' {
						out[i] = '\n'
						i++
						continue
					}
					if strings.HasPrefix(src[i:], tag) {
						i += len(tag)
						break
					}
					i++
				}
				continue
			}
		}
		out[i] = src[i]
		i++
	}
	return string(out)
}

func matchDollarTag(s string) string {
	if len(s) < 2 || s[0] != '$' {
		return ""
	}
	if s[1] == '$' {
		return "$$"
	}
	for i := 1; i < len(s); i++ {
		c := s[i]
		if c == '$' {
			return s[:i+1]
		}
		isAlphaNum := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
		if !isAlphaNum {
			return ""
		}
	}
	return ""
}

func collapseWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// SplitNoTransactionStatements splits a migration file into individual
// executable SQL statements when it carries `-- sorotrail:no-transaction`.
// Unlike the static linter helper, it slices the original SQL text at
// top-level semicolon boundaries so string literals (including semicolons) and
// dollar-quoted function bodies are preserved verbatim.
func SplitNoTransactionStatements(sqlContent string) []string {
	var out []string
	n := len(sqlContent)
	stmtStart := 0
	hasCode := false
	i := 0

	for i < n {
		c := sqlContent[i]
		// Line comment: -- ... \n
		if i+1 < n && c == '-' && sqlContent[i+1] == '-' {
			if !hasCode {
				for i < n && sqlContent[i] != '\n' {
					i++
				}
				if i < n && sqlContent[i] == '\n' {
					i++
				}
				stmtStart = i
				continue
			}
			for i < n && sqlContent[i] != '\n' {
				i++
			}
			continue
		}
		// Block comment: /* ... */
		if i+1 < n && c == '/' && sqlContent[i+1] == '*' {
			i += 2
			for i < n {
				if i+1 < n && sqlContent[i] == '*' && sqlContent[i+1] == '/' {
					i += 2
					break
				}
				i++
			}
			if !hasCode {
				stmtStart = i
			}
			continue
		}
		// Single-quoted string literal: '...' (with '' escapes)
		if c == '\'' {
			hasCode = true
			i++
			for i < n {
				if sqlContent[i] == '\'' {
					i++
					if i < n && sqlContent[i] == '\'' {
						i++
						continue
					}
					break
				}
				i++
			}
			continue
		}
		// Dollar-quoted body: $$ ... $$ or $tag$ ... $tag$
		if c == '$' {
			if tag := matchDollarTag(sqlContent[i:]); tag != "" {
				hasCode = true
				i += len(tag)
				for i < n {
					if strings.HasPrefix(sqlContent[i:], tag) {
						i += len(tag)
						break
					}
					i++
				}
				continue
			}
		}
		// Top-level statement terminator
		if c == ';' {
			if hasCode {
				if stmt := strings.TrimSpace(sqlContent[stmtStart:i]); stmt != "" {
					out = append(out, stmt)
				}
			}
			i++
			stmtStart = i
			hasCode = false
			continue
		}
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
			hasCode = true
		}
		i++
	}

	if hasCode {
		if tail := strings.TrimSpace(sqlContent[stmtStart:]); tail != "" {
			out = append(out, tail)
		}
	}
	return out
}

// LintMigrationFile inspects a single migration file (`filename` and `sqlContent`)
// against the live-database safety rules. When allowHistoricalExceptions is true,
// documented pre-#1228 exceptions in HistoricalMigrationExceptions are filtered out.
func LintMigrationFile(filename, sqlContent string, allowHistoricalExceptions bool) []MigrationViolation {
	isDown := strings.HasSuffix(filename, ".down.sql")
	noTx := HasNoTransactionDirective(sqlContent)
	allowDropColumn := hasHeaderDirective(sqlContent, DirectiveAllowContractDropColumn)
	allowPartitionedIndex := hasHeaderDirective(sqlContent, DirectiveAllowPartitionedParentIndex)

	stmts := parseTopLevelSQLStatements(sqlContent)

	tablesCreatedInFile := make(map[string]bool)
	// notValidConstraintCol maps "table|constraint_name" -> "column_name" for
	// CHECK (<col> IS NOT NULL) NOT VALID constraints added earlier in the file.
	notValidConstraintCol := make(map[string]string)
	// validatedNotNullCols tracks "table|column_name" pairs whose NOT VALID
	// NOT NULL check constraint has been validated via VALIDATE CONSTRAINT.
	validatedNotNullCols := make(map[string]bool)
	hasExplicitTxBlock := false

	for _, st := range stmts {
		if reBeginCommitStmt.MatchString(st.text) {
			hasExplicitTxBlock = true
		}
	}

	var violations []MigrationViolation

	if noTx && len(stmts) > 1 {
		violations = append(violations, MigrationViolation{
			File:      filename,
			Line:      stmts[1].line,
			Rule:      RuleConcurrentIndexNoTx,
			Statement: stmts[1].text,
			Reason:    "`-- sorotrail:no-transaction` migration files must contain a single CREATE/DROP INDEX CONCURRENTLY statement so PostgreSQL executes it outside an implicit multi-statement transaction block",
		})
	}

	for _, st := range stmts {
		if m := reCreateTableStmt.FindStringSubmatch(st.text); m != nil {
			tablesCreatedInFile[strings.ToLower(m[1])] = true
		}
		isConcurrentIndexStmt := false
		// Check CREATE [UNIQUE] INDEX rules on both .up.sql and .down.sql
		if m := reCreateIndexStmt.FindStringSubmatch(st.text); m != nil {
			concurrent := strings.TrimSpace(m[1]) != "" || strings.Contains(strings.ToUpper(st.text), " INDEX CONCURRENTLY ")
			tbl := strings.ToLower(m[2])
			if concurrent {
				isConcurrentIndexStmt = true
				if !noTx || hasExplicitTxBlock {
					violations = append(violations, MigrationViolation{
						File:      filename,
						Line:      st.line,
						Rule:      RuleConcurrentIndexNoTx,
						Statement: st.text,
						Reason:    "CREATE INDEX CONCURRENTLY requires `-- sorotrail:no-transaction` and cannot run inside BEGIN/COMMIT",
					})
				}
			} else if !isDown && !tablesCreatedInFile[tbl] && !allowPartitionedIndex {
				violations = append(violations, MigrationViolation{
					File:      filename,
					Line:      st.line,
					Rule:      RuleConcurrentIndexRequired,
					Statement: st.text,
					Reason:    fmt.Sprintf("CREATE INDEX on existing live table %q must use CONCURRENTLY (with `-- sorotrail:no-transaction`)", tbl),
				})
			}
		}

		if reDropIndexConc.MatchString(st.text) {
			isConcurrentIndexStmt = true
			if !noTx || hasExplicitTxBlock {
				violations = append(violations, MigrationViolation{
					File:      filename,
					Line:      st.line,
					Rule:      RuleConcurrentIndexNoTx,
					Statement: st.text,
					Reason:    "DROP INDEX CONCURRENTLY requires `-- sorotrail:no-transaction` and cannot run inside BEGIN/COMMIT",
				})
			}
		}

		if noTx && !isConcurrentIndexStmt && !reBeginCommitStmt.MatchString(st.text) {
			violations = append(violations, MigrationViolation{
				File:      filename,
				Line:      st.line,
				Rule:      RuleConcurrentIndexNoTx,
				Statement: st.text,
				Reason:    "`-- sorotrail:no-transaction` migration files may only contain CREATE INDEX CONCURRENTLY or DROP INDEX CONCURRENTLY statements",
			})
		}

		if isDown {
			continue
		}

		// Rules below apply to forward (.up.sql) migrations on live databases.
		if m := reAlterTableHead.FindStringSubmatch(st.text); m != nil {
			tbl := strings.ToLower(m[1])
			body := m[2]
			for _, nvMatch := range reNotValidCheck.FindAllStringSubmatch(body, -1) {
				cName := strings.ToLower(nvMatch[1])
				colName := strings.ToLower(nvMatch[2])
				notValidConstraintCol[tbl+"|"+cName] = colName
			}
			for _, valMatch := range reValidateCheck.FindAllStringSubmatch(body, -1) {
				cName := strings.ToLower(valMatch[1])
				if colName, ok := notValidConstraintCol[tbl+"|"+cName]; ok {
					validatedNotNullCols[tbl+"|"+colName] = true
				}
			}
			if !tablesCreatedInFile[tbl] {
				if reDropColumnClause.MatchString(body) && !allowDropColumn {
					violations = append(violations, MigrationViolation{
						File:      filename,
						Line:      st.line,
						Rule:      RuleNoDropColumn,
						Statement: st.text,
						Reason:    fmt.Sprintf("ALTER TABLE %q DROP COLUMN breaks live instances reading the column; remove all reads/writes first and annotate Stage 5 with `%s <reason>`", tbl, DirectiveAllowContractDropColumn),
					})
				}
				if reRenameClause.MatchString(body) {
					violations = append(violations, MigrationViolation{
						File:      filename,
						Line:      st.line,
						Rule:      RuleNoRenameColumnOrTable,
						Statement: st.text,
						Reason:    fmt.Sprintf("ALTER TABLE %q RENAME breaks live rolling-deploy readers/writers; use the 5-stage expand-and-contract pattern instead", tbl),
					})
				}
				if reAlterTypeClause.MatchString(body) {
					violations = append(violations, MigrationViolation{
						File:      filename,
						Line:      st.line,
						Rule:      RuleNoAlterColumnType,
						Statement: st.text,
						Reason:    fmt.Sprintf("ALTER TABLE %q ALTER COLUMN ... TYPE rewrites the table under ACCESS EXCLUSIVE lock; use expand-and-contract instead", tbl),
					})
				}
				for _, snnMatch := range reSetNotNullClause.FindAllStringSubmatch(body, -1) {
					colName := strings.ToLower(snnMatch[1])
					if !validatedNotNullCols[tbl+"|"+colName] {
						violations = append(violations, MigrationViolation{
							File:      filename,
							Line:      st.line,
							Rule:      RuleNotNullNeedsDefault,
							Statement: st.text,
							Reason:    fmt.Sprintf("ALTER TABLE %q ALTER COLUMN %q SET NOT NULL scans the table under ACCESS EXCLUSIVE unless preceded by a NOT VALID + VALIDATE CONSTRAINT check on %q", tbl, colName, colName),
						})
					}
				}
				for _, addMatch := range reAddColumnClause.FindAllStringSubmatch(body, -1) {
					colName := addMatch[1]
					switch strings.ToUpper(colName) {
					case "CONSTRAINT", "PRIMARY", "UNIQUE", "FOREIGN", "CHECK":
						continue
					}
					colSpec := strings.ToUpper(addMatch[2])
					if strings.Contains(colSpec, "NOT NULL") && !strings.Contains(colSpec, "DEFAULT") {
						violations = append(violations, MigrationViolation{
							File:      filename,
							Line:      st.line,
							Rule:      RuleNotNullNeedsDefault,
							Statement: st.text,
							Reason:    fmt.Sprintf("ALTER TABLE %q ADD COLUMN %q NOT NULL without DEFAULT fails on non-empty live tables", tbl, colName),
						})
					}
				}
			}
		}

		// Unbounded DML backfill rules on existing tables.
		if m := reUpdateStmt.FindStringSubmatch(st.text); m != nil {
			tbl := strings.ToLower(m[1])
			if !tablesCreatedInFile[tbl] {
				violations = append(violations, MigrationViolation{
					File:      filename,
					Line:      st.line,
					Rule:      RuleNoUnboundedDataBackfill,
					Statement: st.text,
					Reason:    fmt.Sprintf("unbounded UPDATE on existing table %q inside a DDL migration holds locks and risks statement_timeout; use RunBatchedMigration", tbl),
				})
			}
		}
		if m := reDeleteStmt.FindStringSubmatch(st.text); m != nil {
			tbl := strings.ToLower(m[1])
			if !tablesCreatedInFile[tbl] {
				violations = append(violations, MigrationViolation{
					File:      filename,
					Line:      st.line,
					Rule:      RuleNoUnboundedDataBackfill,
					Statement: st.text,
					Reason:    fmt.Sprintf("unbounded DELETE on existing table %q inside a DDL migration holds locks and risks statement_timeout; use RunBatchedMigration", tbl),
				})
			}
		}
		if m := reInsertSelectStmt.FindStringSubmatch(st.text); m != nil {
			targetTbl := strings.ToLower(m[1])
			sourceTbl := strings.ToLower(m[2])
			readsExistingTable := sourceTbl != "" && !tablesCreatedInFile[sourceTbl]
			if !tablesCreatedInFile[targetTbl] || readsExistingTable {
				violations = append(violations, MigrationViolation{
					File:      filename,
					Line:      st.line,
					Rule:      RuleNoUnboundedDataBackfill,
					Statement: st.text,
					Reason:    fmt.Sprintf("unbounded INSERT INTO %q ... SELECT backfill inside a DDL migration holds locks and risks statement_timeout; use RunBatchedMigration", targetTbl),
				})
			}
		}
	}

	if !allowHistoricalExceptions || len(violations) == 0 {
		return violations
	}

	exceptionSet := make(map[string]bool, len(HistoricalMigrationExceptions))
	for _, ex := range HistoricalMigrationExceptions {
		exceptionSet[ex.File+"|"+string(ex.Rule)] = true
	}

	var filtered []MigrationViolation
	for _, v := range violations {
		if exceptionSet[v.File+"|"+string(v.Rule)] {
			continue
		}
		filtered = append(filtered, v)
	}
	return filtered
}

// LintEmbeddedMigrations validates every embedded PostgreSQL migration file
// against the live-database safety rules and ensures no stale entries remain
// in HistoricalMigrationExceptions.
func LintEmbeddedMigrations() ([]MigrationViolation, error) {
	entries, err := postgresMigrationsFS.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("reading embedded migrations: %w", err)
	}

	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	var allViolations []MigrationViolation
	triggeredExceptions := make(map[string]bool)

	for _, name := range names {
		raw, err := postgresMigrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return nil, fmt.Errorf("reading migration %s: %w", name, err)
		}
		content := string(raw)

		rawViolations := LintMigrationFile(name, content, false)
		for _, v := range rawViolations {
			triggeredExceptions[v.File+"|"+string(v.Rule)] = true
		}

		filtered := LintMigrationFile(name, content, true)
		allViolations = append(allViolations, filtered...)
	}

	for _, ex := range HistoricalMigrationExceptions {
		key := ex.File + "|" + string(ex.Rule)
		if !triggeredExceptions[key] {
			allViolations = append(allViolations, MigrationViolation{
				File:   ex.File,
				Line:   1,
				Rule:   ex.Rule,
				Reason: "stale HistoricalMigrationExceptions entry did not match any violation in file",
			})
		}
	}

	return allViolations, nil
}
