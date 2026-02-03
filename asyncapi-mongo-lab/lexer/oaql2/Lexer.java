package oaql2;

import java.io.IOException;
import java.io.Reader;
import java_cup.runtime.Symbol;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Lexer {
  private static final Pattern NUM_LITERAL = Pattern.compile("[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?");

  private final String input;
  private int index = 0;
  private int line = 0;
  private int column = 0;

  public Lexer(Reader reader) throws IOException {
    StringBuilder builder = new StringBuilder();
    char[] buffer = new char[2048];
    int read;
    while ((read = reader.read(buffer)) != -1) {
      builder.append(buffer, 0, read);
    }
    this.input = builder.toString();
  }

  public Symbol next_token() {
    skipWhitespace();

    if (index >= input.length()) {
      return new Symbol(sym.EOF, line, column);
    }

    char current = input.charAt(index);

    if (current == '"') {
      return readString();
    }

    if (current == '(') {
      advance();
      return new Symbol(sym.LP, line, column - 1);
    }
    if (current == ')') {
      advance();
      return new Symbol(sym.RP, line, column - 1);
    }
    if (current == ',') {
      advance();
      return new Symbol(sym.COMMA, line, column - 1);
    }
    if (current == '.') {
      advance();
      return new Symbol(sym.DOT, line, column - 1);
    }
    if (current == '*') {
      advance();
      return new Symbol(sym.STAR, line, column - 1);
    }

    if (matchOperator("<>")) {
      return new Symbol(sym.OPERATOR, line, column - 2, "$ne");
    }
    if (matchOperator(">=")) {
      return new Symbol(sym.OPERATOR, line, column - 2, "$gte");
    }
    if (matchOperator("<=")) {
      return new Symbol(sym.OPERATOR, line, column - 2, "$lte");
    }
    if (matchOperator("=")) {
      return new Symbol(sym.OPERATOR, line, column - 1, "$eq");
    }
    if (matchOperator(">")) {
      return new Symbol(sym.OPERATOR, line, column - 1, "$gt");
    }
    if (matchOperator("<")) {
      return new Symbol(sym.OPERATOR, line, column - 1, "$lt");
    }

    if (isIdentifierStart(current)) {
      String identifier = readIdentifier();
      String upper = identifier.toUpperCase();

      if (peek('.') && isIdentifierStart(peekChar(1))) {
        advance();
        String right = readIdentifier();
        return new Symbol(sym.FIELD, line, column - right.length() - 1, identifier + "." + right);
      }

      if ("SELECT".equals(upper)) {
        return new Symbol(sym.SELECT, line, column - identifier.length());
      }
      if ("DISTINCT".equals(upper)) {
        return new Symbol(sym.DISTINCT, line, column - identifier.length());
      }
      if ("FROM".equals(upper)) {
        return new Symbol(sym.FROM, line, column - identifier.length());
      }
      if ("JOIN".equals(upper)) {
        return new Symbol(sym.JOIN, line, column - identifier.length());
      }
      if ("ON".equals(upper)) {
        return new Symbol(sym.ON, line, column - identifier.length());
      }
      if ("AS".equals(upper)) {
        return new Symbol(sym.AS, line, column - identifier.length());
      }
      if ("WHERE".equals(upper)) {
        return new Symbol(sym.WHERE, line, column - identifier.length());
      }
      if ("AND".equals(upper)) {
        return new Symbol(sym.AND, line, column - identifier.length());
      }
      if ("OR".equals(upper)) {
        return new Symbol(sym.OR, line, column - identifier.length());
      }
      if ("XOR".equals(upper)) {
        return new Symbol(sym.XOR, line, column - identifier.length());
      }
      if ("NOT".equals(upper)) {
        return new Symbol(sym.NOT, line, column - identifier.length());
      }
      if ("IN".equals(upper)) {
        return new Symbol(sym.IN, line, column - identifier.length());
      }
      if ("IS".equals(upper)) {
        return new Symbol(sym.IS, line, column - identifier.length());
      }
      if ("NULL".equals(upper)) {
        return new Symbol(sym.NULL, line, column - identifier.length());
      }
      if ("BETWEEN".equals(upper)) {
        return new Symbol(sym.BETWEEN, line, column - identifier.length());
      }
      if ("ORDER".equals(upper)) {
        return new Symbol(sym.ORDER, line, column - identifier.length());
      }
      if ("BY".equals(upper)) {
        return new Symbol(sym.BY, line, column - identifier.length());
      }
      if ("LIMIT".equals(upper)) {
        return new Symbol(sym.LIMIT, line, column - identifier.length());
      }
      if ("ASC".equals(upper)) {
        return new Symbol(sym.ASCDESC, line, column - identifier.length(), 1);
      }
      if ("DESC".equals(upper)) {
        return new Symbol(sym.ASCDESC, line, column - identifier.length(), -1);
      }
      if ("LIKE".equals(upper)) {
        return new Symbol(sym.LIKE, line, column - identifier.length());
      }
      if ("TRUE".equals(upper)) {
        return new Symbol(sym.BOOL_VALUE, line, column - identifier.length(), true);
      }
      if ("FALSE".equals(upper)) {
        return new Symbol(sym.BOOL_VALUE, line, column - identifier.length(), false);
      }

      return new Symbol(sym.IDENTIFIER, line, column - identifier.length(), identifier);
    }

    if (isNumberStart(current)) {
      String numberText = readNumber();
      return new Symbol(sym.NUM_VALUE, line, column - numberText.length(), Double.parseDouble(numberText));
    }

    throw new Error(
      "Illegal character <" + current + "> at line " + line + ", column " + column
    );
  }

  private void skipWhitespace() {
    while (index < input.length()) {
      char current = input.charAt(index);
      if (current == ' ' || current == '\t' || current == '\r' || current == '\n' || current == '\f') {
        advance();
        continue;
      }
      break;
    }
  }

  private Symbol readString() {
    int startLine = line;
    int startColumn = column;
    advance();
    StringBuilder builder = new StringBuilder();

    while (index < input.length()) {
      char current = input.charAt(index);
      if (current == '"') {
        advance();
        return new Symbol(sym.STRING_VALUE, startLine, startColumn, builder.toString());
      }

      if (current == '\\') {
        if (index + 1 >= input.length()) {
          break;
        }
        char escape = input.charAt(index + 1);
        switch (escape) {
          case 't':
            builder.append('\t');
            break;
          case 'n':
            builder.append('\n');
            break;
          case 'r':
            builder.append('\r');
            break;
          case '"':
            builder.append('"');
            break;
          case '\\':
            builder.append('\\');
            break;
          default:
            builder.append(escape);
            break;
        }
        advance();
        advance();
        continue;
      }

      builder.append(current);
      advance();
    }

    throw new Error("Unterminated string literal at line " + startLine + ", column " + startColumn);
  }

  private boolean matchOperator(String operator) {
    if (input.startsWith(operator, index)) {
      for (int i = 0; i < operator.length(); i++) {
        advance();
      }
      return true;
    }
    return false;
  }

  private boolean isIdentifierStart(char value) {
    return Character.isLetter(value) || value == '_';
  }

  private boolean isIdentifierPart(char value) {
    return Character.isLetterOrDigit(value) || value == '_' || value == '-';
  }

  private String readIdentifier() {
    int start = index;
    while (index < input.length() && isIdentifierPart(input.charAt(index))) {
      advance();
    }
    return input.substring(start, index);
  }

  private boolean isNumberStart(char value) {
    if (Character.isDigit(value)) {
      return true;
    }
    if ((value == '-' || value == '+') && index + 1 < input.length()) {
      char next = input.charAt(index + 1);
      return Character.isDigit(next) || next == '.';
    }
    return value == '.' && index + 1 < input.length() && Character.isDigit(input.charAt(index + 1));
  }

  private String readNumber() {
    String remaining = input.substring(index);
    Matcher matcher = NUM_LITERAL.matcher(remaining);
    if (!matcher.find()) {
      throw new Error("Invalid numeric literal at line " + line + ", column " + column);
    }
    String value = matcher.group();
    for (int i = 0; i < value.length(); i++) {
      advance();
    }
    return value;
  }

  private boolean peek(char expected) {
    return index < input.length() && input.charAt(index) == expected;
  }

  private char peekChar(int offset) {
    int target = index + offset;
    if (target >= input.length()) {
      return '\0';
    }
    return input.charAt(target);
  }

  private void advance() {
    if (index >= input.length()) {
      return;
    }
    char current = input.charAt(index);
    index++;
    if (current == '\n') {
      line++;
      column = 0;
    } else {
      column++;
    }
  }
}
