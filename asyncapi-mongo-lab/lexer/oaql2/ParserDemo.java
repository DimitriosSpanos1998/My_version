package oaql2;

import java.io.IOException;
import java.io.StringReader;

public class ParserDemo {
  public static void main(String[] args) throws IOException {
    String exampleQuery = "SELECT name, age FROM users WHERE age >= 18";

    Lexer lexer = new Lexer(new StringReader(exampleQuery));
    SimpleParser parser = new SimpleParser(lexer);

    try {
      parser.parseQuery();
      System.out.println("VALID");
    } catch (SimpleParser.ParseException error) {
      System.out.println("INVALID: " + error.getMessage());
    }
  }
}
