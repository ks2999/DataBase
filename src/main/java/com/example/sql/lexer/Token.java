package com.example.sql.lexer;

/**
 * Токен SQL
 */
public class Token {
    public enum Type {
        // Ключевые слова
        CREATE, TABLE, INSERT, INTO, VALUES, SELECT, FROM, WHERE,
        AND, OR, NOT, AS, INDEX, ON, DROP,
        // Операторы
        EQ, NE, LT, LE, GT, GE, PLUS, MINUS, STAR, SLASH,
        // Знаки препинания
        COMMA, SEMICOLON, LPAREN, RPAREN, DOT,
        // Литералы
        IDENTIFIER, STRING, NUMBER,
        // Специальные
        EOF, UNKNOWN
    }
    
    private Type type;
    private String value;
    private int line;
    private int column;
    
    public Token(Type type, String value, int line, int column) {
        this.type = type;
        this.value = value;
        this.line = line;
        this.column = column;
    }
    
    public Type getType() {
        return type;
    }
    
    public String getValue() {
        return value;
    }
    
    public int getLine() {
        return line;
    }
    
    public int getColumn() {
        return column;
    }
    
    @Override
    public String toString() {
        return String.format("Token(%s, '%s', %d:%d)", type, value, line, column);
    }
}

