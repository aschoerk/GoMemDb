
/* Infix notation calculator.  */

%{

package parser

import (
    "bufio"
    "strings"
    "time"
    . "github.com/aschoerk/go-sql-mem/data"
    "database/sql/driver"
)

type yyLexerEx interface {
	yyLexer
	StoreLvals(yySymType)
}

%}

%union{
    value float64
    column GoSqlColumn
    columns []GoSqlColumn
    int int
    boolean bool
    token int
    string string
    time time.Time
    fieldList []string
    valueList []driver.Value
    termLists [][]*GoSqlTerm
    orderByEntryList []GoSqlOrderBy
    orderByEntry GoSqlOrderBy
    float64 float64
    ptr *Ptr
    term *GoSqlTerm
    termList []*GoSqlTerm
    updateSpec GoSqlUpdateSpec
    updateSpecs []GoSqlUpdateSpec
    selectList []SelectListEntry
    selectListEntry SelectListEntry
    updateRequest GoSqlUpdateRequest
    deleteRequest GoSqlDeleteRequest
    parseResult driver.Stmt
}

// DDL
%token CREATE DATABASE SCHEMA ALTER TABLE ADD AS IF NOT EXISTS PRIMARY KEY AUTOINCREMENT
%token <token> CHAR VARCHAR INTEGER FLOAT TEXT BOOLEAN TIMESTAMP
// DML
%token SELECT DISTINCT ALL FROM WHERE GROUP BY HAVING ORDER ASC DESC UNION BETWEEN BETWEEN_AND AND IN INSERT UPDATE SET DELETE INTO VALUES
%token LESS_OR_EQUAL GREATER_OR_EQUAL NOT_EQUAL PLUS MINUS LESS EQUAL GREATER MULTIPLY DIVIDE AND OR LIKE MOD
%token NUM ISNULL ISNOTNULL NULL IS
%token <int> DECIMAL_INTEGER_NUMBER POSITIVE_DECIMAL_INTEGER_NUMBER 
%token <string> IDENTIFIER PLACEHOLDER STRING
%token <float64> FLOATING_POINT_NUMBER
%token <time> TIME_STAMP
%token <boolean> TRUE, FALSE

%left IS
%left IN
%left OR
%left AND
%right NOT
%left NOT_EQUAL EQUAL
%left	LESS_OR_EQUAL GREATER_OR_EQUAL LESS GREATER
%left LIKE 
%left BETWEEN BETWEEN_AND
%left	PLUS MINUS
%left	MULTIPLY DIVIDE MOD
%left	NEG     /* negation--unary minus */

%type <column> column
%type <columns> columns
%type <fieldList> field_list
%type <valueList> identifier_or_number_list opt_group_by 
%type <termLists> term_lists

%type <token> column_type 
%type <int>  opt_column_length column_specification2 if_exists_predicate distinct_all
%type <ptr> const_expression like_term 
%type <termList> term_list
%type <parseResult> statement ddl_statement dml_statement create_table select insert update delete
%type <selectList> select_list
%type <selectListEntry> select_list_entry
%type <string> select_list_entry_alias
%type <string> from_spec
%type <term> term nonboolean_term opt_where opt_having
%type <orderByEntry> order_by_entry
%type <orderByEntryList> order_by_entry_list opt_order_by
%type <token> order_by_direction
%type <updateSpec> update_spec
%type <updateSpecs> update_specs

%% /* The grammar follows.  */

start: statement
    { 
      setParseResult(yylex, $1)
    }

statement: ddl_statement
            { $$ = $1 }
        | dml_statement
            { $$ = $1 }

ddl_statement:
        CREATE DATABASE if_exists_predicate IDENTIFIER 
            { $$ = &GoSqlCreateDatabaseRequest{NewStatementBaseData(), $3, $4} }
        | CREATE SCHEMA if_exists_predicate IDENTIFIER 
            { $$ = &GoSqlCreateSchemaRequest{NewStatementBaseData(), $3, $4} }
        | create_table
            { $$ = $1 }


create_table:
    CREATE TABLE if_exists_predicate IDENTIFIER '(' columns ')'
        { $$ = &GoSqlCreateTableRequest {NewStatementBaseData(),$3, NewTable($4, $6) } }

columns: column
       { $$ = []GoSqlColumn{$1}}
    | columns ',' column
      { $$ = append($1, $3) }

column: IDENTIFIER column_type opt_column_length column_specification2
    { $$ = NewColumn($1, $2, $3, $4) }


column_specification2: /* EMPTY */
    { $$ = -1}
    | PRIMARY KEY AUTOINCREMENT { $$ = PRIMARY_AUTOINCREMENT }

column_type: INTEGER | TEXT | VARCHAR | BOOLEAN | TIMESTAMP | FLOAT

opt_column_length:
    { $$ = -1 }
    | '(' POSITIVE_DECIMAL_INTEGER_NUMBER ')' { $$ = $2 }

if_exists_predicate: /* empty */
        { $$ = -1 }
    | IF EXISTS
        { $$ = 0 }
    | IF NOT EXISTS
        { $$ = 1 }

dml_statement:
        select
            { $$ = $1 }
        | insert
            { $$ = $1 }
        | update
            { $$ = $1 }
        | delete
            { $$ = $1 }

delete: DELETE FROM from_spec opt_where
    { $$ = &GoSqlDeleteRequest{NewStatementBaseData(),$3, $4}}            

update: UPDATE IDENTIFIER SET update_specs opt_where
    { $$ = NewUpdateRequest($2, $4, $5) }

update_specs: update_spec
      { $$ = []GoSqlUpdateSpec{$1}}
    | update_specs ',' update_spec
      { $$ = append($1, $3) }


update_spec: IDENTIFIER EQUAL term
   { $$ = GoSqlUpdateSpec{ $1, $3 }}


select: SELECT 
       { setExtraState(yylex, SELECT)}
          distinct_all select_list
        { setExtraState(yylex, FROM)}
        FROM from_spec
        { setExtraState(yylex, WHERE)}         
        opt_where 
        { setExtraState(yylex, GROUP)}
        opt_group_by 
        { setExtraState(yylex, HAVING)}
        opt_having
        opt_order_by
  { $$ = &GoSqlSelectRequest { NewStatementBaseData(), $3, $4, $7, $9, $11, $13, $14 }}

distinct_all: 
   { $$ = ALL }
  | ALL
   { $$ = ALL }
  | DISTINCT
   { $$ = DISTINCT }

select_list: 
    select_list_entry
    { $$ = []SelectListEntry{$1}}
    | select_list ',' select_list_entry
    { $$ = append($1, $3) }

select_list_entry:
    '*'
    { $$ = NewSelectListEntry(true, nil, "")}
    | term select_list_entry_alias
    { $$ = NewSelectListEntry(false, $1, $2)}

select_list_entry_alias: 
    { $$ = "" }
    | AS IDENTIFIER
    { $$ = $2}


from_spec: IDENTIFIER { $$ = $1 }

opt_where:
    { $$ = nil}
  | WHERE term
    { $$ = $2 }
  
nonboolean_term:   
    const_expression
    { $$ = &GoSqlTerm{-1, nil, nil, $1}}
  | term PLUS term
    { $$ = &GoSqlTerm{ PLUS, $1, $3, nil }}
  | term MINUS term
    { $$ = &GoSqlTerm{ MINUS, $1, $3, nil }}
  | term MULTIPLY term
    { $$ = &GoSqlTerm{ MULTIPLY, $1, $3, nil }}
  | term DIVIDE term
    { $$ = &GoSqlTerm{ DIVIDE, $1, $3, nil }}
  | term MOD term
    { $$ = &GoSqlTerm{ MOD, $1, $3, nil }}

like_term: 
    PLACEHOLDER 
      { $$ = &Ptr {$1,PLACEHOLDER} } 
    | STRING
      { $$ = &Ptr {$1,STRING} }

term:
    nonboolean_term
  | term AND term
    { $$ = &GoSqlTerm{ AND, $1, $3, nil }}
  | term OR term
    { $$ = &GoSqlTerm{ OR, $1, $3, nil }}
  | NOT term
    { $$ = &GoSqlTerm{ NOT, $2, nil, nil }}
  | term IS NULL
    { $$ = &GoSqlTerm{ ISNULL, $1, nil, nil }}
  | term IS NOT NULL
    { $$ = &GoSqlTerm{ ISNOTNULL, $1, nil, nil }}
  | '(' term ')'
    { $$ = $2 }
  | term LIKE like_term
    {  $$ = &GoSqlTerm { LIKE, $1, &GoSqlTerm { -1, nil, nil, $3 }, nil} }
  | term BETWEEN nonboolean_term BETWEEN_AND term
    { $$ = &GoSqlTerm { BETWEEN, $1, &GoSqlTerm { -1, $3, $5, nil }, nil}}
  | term LESS term
    { $$ = &GoSqlTerm{ LESS, $1, $3, nil }}
  | term LESS_OR_EQUAL term
    { $$ = &GoSqlTerm{ LESS_OR_EQUAL, $1, $3, nil }}
  | term EQUAL term
    { $$ = &GoSqlTerm{ EQUAL, $1, $3, nil }}
  | term GREATER term
    { $$ = &GoSqlTerm{ GREATER, $1, $3, nil }}
  | term GREATER_OR_EQUAL term
    { $$ = &GoSqlTerm{ GREATER_OR_EQUAL, $1, $3, nil }}
  | term NOT_EQUAL term
    { $$ = &GoSqlTerm{ NOT_EQUAL, $1, $3, nil }}
  | term IN '(' select ')'
    { panic("not implemented")}



opt_group_by:
  { $$ = nil}
  | GROUP BY identifier_or_number_list
  { $$ = $3}

opt_order_by:
  { $$ = nil}
  | ORDER BY order_by_entry_list
  { $$ = $3}


order_by_direction:
    { $$ = ASC }
  | ASC
    { $$ = ASC }
  | DESC
    { $$ = DESC}


order_by_entry: IDENTIFIER order_by_direction
        { $$ = GoSqlOrderBy {$1, $2 } }
    | POSITIVE_DECIMAL_INTEGER_NUMBER  order_by_direction
        { $$ = GoSqlOrderBy {$1, $2 } }

order_by_entry_list: order_by_entry
        { $$ = []GoSqlOrderBy{$1}}
  | order_by_entry_list ',' order_by_entry
        { $$ = append($1, $3)}

identifier_or_number_list: IDENTIFIER
        { $$ = []driver.Value{$1} }
    | POSITIVE_DECIMAL_INTEGER_NUMBER
        { $$ = []driver.Value{$1} }
    | identifier_or_number_list ',' IDENTIFIER
    { $$ = append($1, $3)}
    | identifier_or_number_list ',' POSITIVE_DECIMAL_INTEGER_NUMBER
    { $$ = append($1, $3)}

opt_having:
  { $$ = nil}
  | HAVING term
  { $$ = $2}

update: UPDATE
    { $$ = nil }

insert: INSERT INTO IDENTIFIER '(' field_list ')' VALUES term_lists
  { $$ = NewInsertRequest($3, $5, $8) }

term_lists: '(' term_list ')'
   { $$ = [][]*GoSqlTerm{$2} }
  | term_lists ',' '(' term_list ')'
   { $$ = append($1,$4) }

field_list: IDENTIFIER
        { $$ = []string{$1} }
    | field_list ',' IDENTIFIER
        { $$ = append($1, $3) }

term_list: term
    { $$ = []*GoSqlTerm{$1} }
    | term_list ',' term
    { $$ = append($1, $3)}

const_expression:
    PLACEHOLDER
    { $$ = &Ptr {$1,PLACEHOLDER} }
    | IDENTIFIER
    { $$ = &Ptr {$1,IDENTIFIER} }
    | DECIMAL_INTEGER_NUMBER
    { $$ = &Ptr {int64($1),INTEGER} }
    | POSITIVE_DECIMAL_INTEGER_NUMBER
    { $$ = &Ptr {int64($1),INTEGER} }
    | FLOATING_POINT_NUMBER
    { $$ = &Ptr {$1,FLOAT} }
    | STRING
    { $$ = &Ptr {$1,STRING} }
    | TIME_STAMP
    { $$ = &Ptr {$1,TIMESTAMP} }





%%

func setParseResult(yylex yyLexer, parseResult driver.Stmt) {
  yylex.(*yylexer).parseResult = parseResult
}

func setExtraState(yylex yyLexer, extraState int) {
  yylex.(*yylexer).extraState = extraState
}

func getExtraState(yylex yyLexer) int {
  return yylex.(*yylexer).extraState
}



func Parse(sql string) (driver.Stmt, int) {
    yyDebug = 1
    yyErrorVerbose = true
    lexer := newLexer(bufio.NewReader(strings.NewReader(sql)))
    res := yyNewParser().Parse(lexer)
    return lexer.parseResult, res
}
