package oaql2;

public final class sym {
  public static final int EOF = 0;
  public static final int LP = 1;
  public static final int RP = 2;
  public static final int COMMA = 3;
  public static final int DOT = 4;
  public static final int STAR = 5;
  public static final int OPERATOR = 6;
  public static final int SELECT = 7;
  public static final int DISTINCT = 8;
  public static final int FROM = 9;
  public static final int JOIN = 10;
  public static final int ON = 11;
  public static final int AS = 12;
  public static final int WHERE = 13;
  public static final int AND = 14;
  public static final int OR = 15;
  public static final int XOR = 16;
  public static final int NOT = 17;
  public static final int IN = 18;
  public static final int IS = 19;
  public static final int NULL = 20;
  public static final int BETWEEN = 21;
  public static final int ORDER = 22;
  public static final int BY = 23;
  public static final int LIMIT = 24;
  public static final int ASCDESC = 25;
  public static final int LIKE = 26;
  public static final int BOOL_VALUE = 27;
  public static final int FIELD = 28;
  public static final int IDENTIFIER = 29;
  public static final int NUM_VALUE = 30;
  public static final int STRING_VALUE = 31;

  private sym() {
  }
}
