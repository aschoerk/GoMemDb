%top {

package parser

import (
    "bufio"
    "fmt"
    "log"
    "io"
    "os"
    "strconv"
    "strings"
    "time"
    "database/sql/driver"
)

type YYcontext struct {
    lval *yySymType
    between_flag bool
}

type YYtype int

}


%{


type yylexer struct{
    src     *bufio.Reader
    buf     []byte
    empty   bool
    parseResult  driver.Stmt
    extraState int
    yy      Scanner
}

var (
    line_num int
)

func newLexer(src *bufio.Reader) (y *yylexer) {
    scanner := Scanner{
        In:            src,
		Wrap:          func(yyy *Scanner) bool { return true },
		IsInteractive: func(file io.Reader) bool { return false },
		bufSize:       yyBufSize,
		chBuf:         make([]byte, yyBufSize+2),
		start:         1,
		stateBuf:      make([]int, yyBufSize+2),
		atBol:         1,
		debug:         yyFlexDebug,
		fillBuffer:    false,
	}

    y = &yylexer{src: src, yy: scanner}
    return
}

func (y yylexer) Error(e string) {
    log.Panic(e)
}

/* types need to also set lval */
func res(lval *yySymType, tokentype YYtype) YYtype {
    lval.token = int(tokentype)
    return tokentype
}

func (y *yylexer) Lex(lval *yySymType) int {
    y.yy.Context = YYcontext{lval, false}
    res :=  y.yy.Lex()
    return int(res)
}
%}

%s BETWEEN_CONDITION

WHITESPACE [ \t]+
NEWLINE    \r?\n
IDENTIFIER        [a-zA-Z][a-zA-Z0-9_]*
QUOTED_IDENTIFIER \"([^"\n]|\"\")+\"
STRING_CONSTANT \'([^'\n]|\'\')*\'
POSITIVE_DECIMAL_INTEGER_NUMBER [0-9]+
FLOATING_POINT_NUMBER ^[-+]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][-+]?[0-9]+)?$
DECIMAL_INTEGER_NUMBER ^[-+]?[0-9]+
XML_TIMESTAMP ^(-[1-9][0-9]*|[1-9][0-9]{3,}|[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](2[0-3]|[01][0-9]):[0-5][0-9])?$


%%

CREATE { return CREATE }
DATABASE { return DATABASE }
SCHEMA { return SCHEMA }
ALTER { return ALTER }
TABLE { return TABLE }
ADD { return ADD }
AS { return AS }
IF { return IF }
NOT { return NOT }
<INITIAL>AND { return AND }
OR { return OR }
EXISTS { return EXISTS }
VALUES { return VALUES }
PRIMARY { return PRIMARY }
KEY { return KEY }
AUTOINCREMENT { return AUTOINCREMENT }
TRUE { yy.Context.lval.boolean = true; return BOOLEAN }
FALSE { yy.Context.lval.boolean = false; return BOOLEAN }

BOOLEAN { return res(yy.Context.lval, BOOLEAN) }
CHAR { return res(yy.Context.lval, CHAR) }
VARCHAR { return res(yy.Context.lval, VARCHAR) }
INTEGER { return res(yy.Context.lval, INTEGER) }
FLOAT { return res(yy.Context.lval, FLOAT) }
TEXT { return res(yy.Context.lval, TEXT) }
TIMESTAMP { return res(yy.Context.lval, TIMESTAMP) }
NULL { return NULL }
IS { return IS }
FOR { return FOR }

SELECT { return SELECT }
DISTINCT { return DISTINCT }
ALL { return ALL }
FROM { return FROM }
WHERE { return WHERE }
GROUP { return GROUP }
BY { return BY }
HAVING { return HAVING }
ORDER { return ORDER }
ASC { return ASC }
DESC { return DESC }
UNION { return UNION }
BETWEEN   { BEGIN BETWEEN_CONDITION; yy.Context.between_flag = true; return BETWEEN }
IN { return IN }
INSERT { return INSERT }
UPDATE { return UPDATE }
DELETE { return DELETE }
INTO { return INTO }
MOD { return MOD }
LIKE { return LIKE }
SET { return SET }
COMMIT { return COMMIT }
"BEGIN" { return BEGIN_TOKEN }
ROLLBACK { return ROLLBACK }
TRANSACTION { return TRANSACTION }
COUNT { return COUNT }
SUM { return SUM }
AVG { return AVG }
MIN { return MIN }
MAX { return MAX }
JOIN { return JOIN }
OUTER { return OUTER }
LEFT { return LEFT }
RIGHT { return RIGHT }
FULL { return FULL }
INNER { return INNER }
CROSS { return CROSS }
ON { return ON }


<BETWEEN_CONDITION>AND    { 
    if yy.Context.between_flag {
        yy.Context.between_flag = false
        BEGIN INITIAL
        return BETWEEN_AND
    } else {
        return AND;
    }
}

{WHITESPACE}    { /* Explicitly ignore horizontal whitespace */ }

{NEWLINE}       { line_num++; /* Count lines, but still ignore newlines */ }

{IDENTIFIER}    {
   yy.Context.lval.string = strings.ToLower(string(yytext));
   return lexDebug(IDENTIFIER, yy, yytext)
 }

{QUOTED_IDENTIFIER}    {
   yy.Context.lval.string =  strings.Replace(string(yytext[1:len(yytext)-1]), `""`, `"`, -1);
   return lexDebug(IDENTIFIER, yy, yytext)
 }


{STRING_CONSTANT}       { yy.Context.lval.string = string(yytext[1:len(yytext)-1]); return lexDebug(STRING, yy, yytext) }

{POSITIVE_DECIMAL_INTEGER_NUMBER}  { yy.Context.lval.int, _ = strconv.Atoi(string(yytext)); return POSITIVE_DECIMAL_INTEGER_NUMBER }

\?|([:@]{IDENTIFIER})|${POSITIVE_DECIMAL_INTEGER_NUMBER}  {
      yy.Context.lval.string = string(yytext);
      return lexDebug(PLACEHOLDER, yy, yytext)
   }

{FLOATING_POINT_NUMBER}  { yy.Context.lval.float64, _ = strconv.ParseFloat(string(yytext),64); return FLOATING_POINT_NUMBER }

{DECIMAL_INTEGER_NUMBER}  { yy.Context.lval.int, _ = strconv.Atoi(string(yytext)); return DECIMAL_INTEGER_NUMBER }

{XML_TIMESTAMP} { yy.Context.lval.time, _ = time.Parse(time.RFC3339Nano,string(yytext)); return TIME_STAMP }


\<\= { return LESS_OR_EQUAL}
\<   { return LESS}
\>   { return GREATER}
\=   { return EQUAL}
\>\= { return GREATER_OR_EQUAL }
\<\> { return NOT_EQUAL }
\+   { return PLUS}
\-   { return MINUS}
\*   { return ASTERISK }
\/   { return DIVIDE }
\(   { return POPEN }
\)   { return PCLOSE }
\,   { return KOMMA }
\.   { return DOT }

%%
  
func lexDebug(token YYtype, yy *Scanner, text []byte) YYtype {
   if yy.debug {
      fmt.Printf("Lex: token: %d, text: %s ", token, string(text))
   }
   return token
}
