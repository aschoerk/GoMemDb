
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

var YYDebug = 1

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
    selectStatement *GoSqlSelectRequest
    selectList []SelectListEntry
    selectListEntry SelectListEntry
    updateRequest GoSqlUpdateRequest
    deleteRequest GoSqlDeleteRequest
    parseResult driver.Stmt
    identifier GoSqlIdentifier
    identifier_list []string
    joined_table *GoSqlJoinedTable
    table_reference *GoSqlTableReference
}

// DDL
%token CREATE DATABASE SCHEMA ALTER TABLE ADD AS IF NOT EXISTS PRIMARY KEY AUTOINCREMENT POPEN PCLOSE COMMA
%token ON
%token <token> CHAR VARCHAR INTEGER FLOAT TEXT BOOLEAN TIMESTAMP FOR
// DML
%token SELECT DISTINCT ALL FROM WHERE GROUP BY HAVING ORDER ASC DESC UNION BETWEEN BETWEEN_AND AND IN INSERT UPDATE SET DELETE INTO VALUES
%token LESS_OR_EQUAL GREATER_OR_EQUAL NOT_EQUAL PLUS MINUS LESS EQUAL GREATER ASTERISK DIVIDE AND OR LIKE MOD
%token NUM ISNULL ISNOTNULL NULL IS 
%token <token> COUNT SUM AVG MIN MAX
%token <int> BEGIN_TOKEN COMMIT ROLLBACK TRANSACTION AUTOCOMMIT ON OFF
%token <int> DECIMAL_INTEGER_NUMBER POSITIVE_DECIMAL_INTEGER_NUMBER 
%token <string> IDENTIFIER PLACEHOLDER STRING
%token <float64> FLOATING_POINT_NUMBER
%token <time> TIME_STAMP
%token <boolean> TRUE, FALSE

%left JOIN INNER CROSS LEFT RIGHT FULL OUTER NATURAL
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
%left	ASTERISK DIVIDE MOD
%left DOT
%left	NEG     /* negation--unary minus */



%type <column> column
%type <columns> columns
%type <fieldList> field_list
%type <termLists> term_lists

%type <token> column_type aggregate_function_name
%type <int>  opt_column_length column_specification2 if_exists_predicate distinct_all
%type <ptr> const_expression like_term 
%type <termList> term_list opt_group_by 
%type <parseResult> statement ddl_statement dml_statement create_table insert update delete connection_level
%type <selectStatement> select
%type <selectList> select_list
%type <selectListEntry> select_list_entry
%type <string> select_list_entry_alias
%type <term> term nonboolean_term opt_where opt_having aggregate_function_parameter
%type <orderByEntry> order_by_entry
%type <orderByEntryList> order_by_entry_list opt_order_by
%type <token> order_by_direction opt_for_update
%type <updateSpec> update_spec
%type <updateSpecs> update_specs
%type <joined_table> joined_table
%type <table_reference> table_reference
%type <token> outer_join_type
%type <token> join_type
%type <identifier> identifier
%type <identifier_list> identifier_list
%type <term> join_specification


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
        CREATE DATABASE if_exists_predicate identifier
            { $$ = &GoSqlCreateDatabaseRequest{NewStatementBaseData(), $3, $4} }
        | CREATE SCHEMA if_exists_predicate identifier
            { $$ = &GoSqlCreateSchemaRequest{NewStatementBaseData(), $3, $4} }
        | create_table
            { $$ = $1 }


create_table:
    CREATE TABLE if_exists_predicate identifier POPEN columns PCLOSE
        {
        $$ = &GoSqlCreateTableRequest {NewStatementBaseData(),$3, NewTable($4, $6) }
        }

columns: column
       { $$ = []GoSqlColumn{$1}}
    | columns COMMA column
      { $$ = append($1, $3) }

column: IDENTIFIER column_type opt_column_length column_specification2
    { $$ = NewColumn($1, $2, $3, $4) }


column_specification2: /* EMPTY */
    { $$ = -1}
    | PRIMARY KEY AUTOINCREMENT { $$ = PRIMARY_AUTOINCREMENT }

column_type: INTEGER | TEXT | VARCHAR | BOOLEAN | TIMESTAMP | FLOAT

opt_column_length:
    { $$ = -1 }
    | POPEN POSITIVE_DECIMAL_INTEGER_NUMBER PCLOSE { $$ = $2 }

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
        | connection_level
            { $$ = $1}

connection_level:
      BEGIN_TOKEN
      { $$ = NewConnectionLevelRequest($1,-1)}
    | BEGIN_TOKEN TRANSACTION
      { $$ = NewConnectionLevelRequest($1,-1)}
    | COMMIT
      { $$ = NewConnectionLevelRequest($1,-1)}
    | COMMIT TRANSACTION
      { $$ = NewConnectionLevelRequest($1,-1)}
    | ROLLBACK
      { $$ = NewConnectionLevelRequest($1,-1)}
    | ROLLBACK TRANSACTION
      { $$ = NewConnectionLevelRequest($1,-1)}
    | SET AUTOCOMMIT ON
      { $$ = NewConnectionLevelRequest($2,$3)}
    | SET AUTOCOMMIT OFF
      { $$ = NewConnectionLevelRequest($2,$3)}

delete: DELETE FROM table_reference opt_where
    { $$ = &GoSqlDeleteRequest{NewStatementBaseData(),$3, $4}}            

update: UPDATE table_reference SET update_specs opt_where
    { $$ = NewUpdateRequest($2, $4, $5) }

update_specs: update_spec
      { $$ = []GoSqlUpdateSpec{$1}}
    | update_specs COMMA update_spec
      { $$ = append($1, $3) }


update_spec: identifier EQUAL term
   { $$ = GoSqlUpdateSpec{ $1, $3 }}


select: SELECT 
          distinct_all select_list
        FROM table_reference
        opt_where 
        opt_group_by 
        opt_having
        opt_order_by
        opt_for_update
  { $$ = &GoSqlSelectRequest { NewStatementBaseData(), $2, $3, $5, $6, $7, $8, $9, $10 }}

distinct_all: 
   { $$ = ALL }
  | ALL
   { $$ = ALL }
  | DISTINCT
   { $$ = DISTINCT }

select_list: 
    select_list_entry
    { $$ = []SelectListEntry{$1}}
    | select_list COMMA select_list_entry
    { $$ = append($1, $3) }

select_list_entry:
    ASTERISK
    { $$ = NewSelectListEntry(true, nil, "")}
    | term select_list_entry_alias
    { $$ = NewSelectListEntry(false, $1, $2)}

select_list_entry_alias: 
    { $$ = "" }
    | AS IDENTIFIER
    { $$ = $2}

table_reference:
    identifier
    { $$ = &GoSqlTableReference{Id: $1} }
    | POPEN select PCLOSE
    { $$ = &GoSqlTableReference{Select: $2} }
    | POPEN joined_table PCLOSE
    { $$ = &GoSqlTableReference{JoinedTable: $2} }
    | table_reference AS IDENTIFIER
    {
      $1.As = $3
      $$ = $1
    }
    | table_reference IDENTIFIER
    {
          $1.As = $2
          $$ = $1
    }
    ;

joined_table:
    table_reference
    { $$ = nil }
    | joined_table COMMA table_reference
    { $$ = nil }
    | joined_table join_type JOIN table_reference join_specification
    { $$ = nil }
    ;

join_specification:
    /* empty */
    { $$ = nil }
    | ON term
    { $$ = $2 }
    ;

join_type:
    /* empty */
    { $$ = 0 }
    | INNER
    { $$ =  INNER }
    | outer_join_type opt_outer
    { $$ =  $1 }
    | CROSS
    { $$ =  CROSS }
    | NATURAL
    { $$ =  NATURAL }
    ;

opt_outer:
   /* empty */
   | OUTER
   ;

outer_join_type:
    LEFT
    { $$ = LEFT }
    | RIGHT
    { $$ = RIGHT }
    | FULL
    { $$ = FULL }
    ;


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
  | term ASTERISK term
    { $$ = &GoSqlTerm{ ASTERISK, $1, $3, nil }}
  | term DIVIDE term
    { $$ = &GoSqlTerm{ DIVIDE, $1, $3, nil }}
  | term MOD term
    { $$ = &GoSqlTerm{ MOD, $1, $3, nil }}
  |  aggregate_function_name POPEN aggregate_function_parameter PCLOSE
    { $$ = &GoSqlTerm{$1, $3, nil, nil} }

aggregate_function_name: 
    COUNT
    { $$ = COUNT }
  | SUM
    { $$ = SUM }
  | AVG
    { $$ = AVG }
  | MIN
    { $$ = MIN }
  | MAX
    { $$ = MAX }


aggregate_function_parameter:  
    distinct_all term 
    { $$ = &GoSqlTerm{$1, $2, nil, nil} }
  | ASTERISK
    { $$ = &GoSqlTerm{ASTERISK, nil, nil, nil} }

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
  | POPEN term PCLOSE
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
  | term IN POPEN select PCLOSE
    { panic("not implemented")}


opt_group_by:
  { $$ = nil}
  | GROUP BY term_list
  { $$ = $3}

opt_order_by:
  { $$ = nil}
  | ORDER BY order_by_entry_list
  { $$ = $3}

opt_for_update:
  { $$ = 0 }
  | FOR UPDATE
  { $$ = $1 }


order_by_direction:
    { $$ = ASC }
  | ASC
    { $$ = ASC }
  | DESC
    { $$ = DESC}

identifier_list:
   IDENTIFIER
   { $$ = []string{$1} }
   | identifier_list DOT IDENTIFIER
   { $$ = append($1,$3) }

identifier:
   identifier_list
   { $$ = GoSqlIdentifier{$1} }

order_by_entry: identifier order_by_direction
        { $$ = GoSqlOrderBy {$1, $2 } }
    | POSITIVE_DECIMAL_INTEGER_NUMBER  order_by_direction
        { $$ = GoSqlOrderBy {$1, $2 } }

order_by_entry_list: order_by_entry
        { $$ = []GoSqlOrderBy{$1}}
  | order_by_entry_list COMMA order_by_entry
        { $$ = append($1, $3)}

opt_having:
  { $$ = nil}
  | HAVING term
  { $$ = $2}

update: UPDATE
    { $$ = nil }

insert: INSERT INTO identifier POPEN field_list PCLOSE VALUES term_lists
  { $$ = NewInsertRequest($3, $5, $8) }

term_lists: POPEN term_list PCLOSE
   { $$ = [][]*GoSqlTerm{$2} }
  | term_lists COMMA POPEN term_list PCLOSE
   { $$ = append($1,$4) }

field_list: IDENTIFIER
        { $$ = []string{$1} }
    | field_list COMMA IDENTIFIER
        { $$ = append($1, $3) }

term_list: term
    { $$ = []*GoSqlTerm{$1} }
    | term_list COMMA term
    { $$ = append($1, $3)}

const_expression:
    PLACEHOLDER
    { $$ = &Ptr {$1,PLACEHOLDER} }
    | identifier
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
    yyDebug = YYDebug
    yyErrorVerbose = true
    lexer := newLexer(bufio.NewReader(strings.NewReader(sql)))
    if yyDebug > 0 {
      lexer.yy.debug = true
    }
    res := yyNewParser().Parse(lexer)
    return lexer.parseResult, res
}
