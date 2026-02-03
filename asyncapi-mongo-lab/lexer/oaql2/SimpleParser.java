package oaql2;

import java.io.IOException;
import java_cup.runtime.Symbol;

public class SimpleParser {
  private final Lexer lexer;
  private Symbol lookahead;

  public SimpleParser(Lexer lexer) {
    this.lexer = lexer;
  }

  public void parseQuery() throws IOException {
    next();
    expect(sym.SELECT, "Expected SELECT");

    if (match(sym.DISTINCT)) {
      next();
    }

    parseSelectList();

    expect(sym.FROM, "Expected FROM");
    parseIdentifier("Expected table name after FROM");

    if (match(sym.WHERE)) {
      next();
      parseCondition();
    }

    expect(sym.EOF, "Unexpected tokens after end of query");
  }

  private void parseSelectList() throws IOException {
    if (match(sym.STAR)) {
      next();
      return;
    }

    parseIdentifier("Expected field name in SELECT list");
    while (match(sym.COMMA)) {
      next();
      parseIdentifier("Expected field name after comma");
    }
  }

  private void parseCondition() throws IOException {
    parseOperand();
    expect(sym.OPERATOR, "Expected comparison operator");
    parseOperand();

    while (match(sym.AND) || match(sym.OR) || match(sym.XOR)) {
      next();
      parseOperand();
      expect(sym.OPERATOR, "Expected comparison operator");
      parseOperand();
    }
  }

  private void parseOperand() throws IOException {
    if (match(sym.IDENTIFIER) || match(sym.FIELD)) {
      next();
      return;
    }

    if (match(sym.STRING_VALUE) || match(sym.NUM_VALUE) || match(sym.BOOL_VALUE) || match(sym.NULL)) {
      next();
      return;
    }

    throw new ParseException("Expected operand but found: " + tokenName());
  }

  private void parseIdentifier(String errorMessage) throws IOException {
    if (match(sym.IDENTIFIER) || match(sym.FIELD)) {
      next();
      return;
    }

    throw new ParseException(errorMessage + " but found: " + tokenName());
  }

  private boolean match(int tokenType) {
    return lookahead != null && lookahead.sym == tokenType;
  }

  private void expect(int tokenType, String errorMessage) throws IOException {
    if (!match(tokenType)) {
      throw new ParseException(errorMessage + " but found: " + tokenName());
    }
    next();
  }

  private void next() throws IOException {
    lookahead = lexer.next_token();
  }

  private String tokenName() {
    if (lookahead == null) {
      return "<none>";
    }
    return "#" + lookahead.sym + (lookahead.value != null ? " (" + lookahead.value + ")" : "");
  }

  public static class ParseException extends RuntimeException {
    public ParseException(String message) {
      super(message);
    }
  }
}
