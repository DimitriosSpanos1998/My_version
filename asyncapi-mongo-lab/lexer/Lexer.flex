package oaql2;

import java_cup.runtime.Symbol;

%%

%class Lexer
%unicode
%cup
%line
%column
%caseless   // keywords case-insensitive: select == SELECT == SeLeCt

%{

  StringBuffer string = new StringBuffer();

  private Symbol symbol(int type) {
    return new Symbol(type, yyline, yycolumn);
  }

  private Symbol symbol(int type, Object value) {
    return new Symbol(type, yyline, yycolumn, value);
  }

%}

/* ---------- Macros / shortcuts ---------- */
Identifier = [A-Za-z_][A-Za-z0-9_\-]*
NumLiteral = [-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?

%state STRING

%%

/********** MAIN STATE: YYINITIAL **********/

<YYINITIAL> {

  /* Delimiters / punctuation */
  "("                 { return symbol(sym.LP); }
  ")"                 { return symbol(sym.RP); }
  ","                 { return symbol(sym.COMMA); }
  "."                 { return symbol(sym.DOT); }
  "*"                 { return symbol(sym.STAR); }

  /* Operators (mapped to something π.χ. Mongo-like) */
  "="                 { return symbol(sym.OPERATOR,"$eq"); }
  "<>"                { return symbol(sym.OPERATOR,"$ne"); }
  ">"                 { return symbol(sym.OPERATOR,"$gt"); }
  ">="                { return symbol(sym.OPERATOR,"$gte"); }
  "<"                 { return symbol(sym.OPERATOR,"$lt"); }
  "<="                { return symbol(sym.OPERATOR,"$lte"); }

  /* Keywords (χάρη στο %caseless δεν έχει σημασία το case) */
  "SELECT"            { return symbol(sym.SELECT); }
  "DISTINCT"          { return symbol(sym.DISTINCT); }
  "FROM"              { return symbol(sym.FROM); }
  "JOIN"              { return symbol(sym.JOIN); }
  "ON"                { return symbol(sym.ON); }
  "AS"                { return symbol(sym.AS); }
  "WHERE"             { return symbol(sym.WHERE); }
  "AND"               { return symbol(sym.AND); }
  "OR"                { return symbol(sym.OR); }
  "XOR"               { return symbol(sym.XOR); }
  "NOT"               { return symbol(sym.NOT); }
  "IN"                { return symbol(sym.IN); }
  "IS"                { return symbol(sym.IS); }
  "NULL"              { return symbol(sym.NULL); }
  "BETWEEN"           { return symbol(sym.BETWEEN); }
  "ORDER"             { return symbol(sym.ORDER); }
  "BY"                { return symbol(sym.BY); }
  "LIMIT"             { return symbol(sym.LIMIT); }
  "ASC"               { return symbol(sym.ASCDESC, 1); }
  "DESC"              { return symbol(sym.ASCDESC, -1); }
  "LIKE"              { return symbol(sym.LIKE); }

  /* Booleans */
  "true"              { return symbol(sym.BOOL_VALUE, true); }
  "false"             { return symbol(sym.BOOL_VALUE, false); }

  /* Fields: alias.field */
  {Identifier}\.{Identifier}  { return symbol(sym.FIELD, yytext()); }

  /* Simple identifiers (π.χ. table, column name) */
  {Identifier}                { return symbol(sym.IDENTIFIER, yytext()); }

  /* Numbers */
  {NumLiteral}         { return symbol(sym.NUM_VALUE, Double.parseDouble(yytext())); }

  /* String literal αρχή */
  \"                   { string.setLength(0); yybegin(STRING); }

  /* Whitespace – αγνόηση */
  [ \t\r\n\f]+         { /* ignore */ }

}

/********** STRING STATE **********/

<STRING> {

  /* Κλείσιμο string */
  \"                   { yybegin(YYINITIAL); return symbol(sym.STRING_VALUE, string.toString()); }

  /* Απλό κείμενο μέσα στο string */
  [^\n\r\t\"\\]+       { string.append(yytext()); }

  /* Escape sequences */
  \\t                  { string.append('\t'); }
  \\n                  { string.append('\n'); }
  \\r                  { string.append('\r'); }
  \\"                 { string.append('\"'); }
  \\                   { string.append('\\'); }

}

/********** ERROR HANDLING **********/

[^] {
  throw new Error(
    "Illegal character <" + yytext() + "> at line " + yyline + ", column " + yycolumn
  );
}
