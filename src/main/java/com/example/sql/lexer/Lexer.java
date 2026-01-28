package com.example.sql.lexer;

import java.util.ArrayList;
import java.util.List;

/**
 * Лексер SQL - преобразование SQL-строки в список токенов
 */
public class Lexer {
    private String input;
    private int pos;
    private int line;
    private int column;
    
    public Lexer(String input) {
        this.input = input != null ? input.trim() : "";
        this.pos = 0;
        this.line = 1;
        this.column = 1;
    }
    
    public List<Token> tokenize() {
        List<Token> tokens = new ArrayList<>();
        Token token;
        
        while ((token = nextToken()).getType() != Token.Type.EOF) {
            tokens.add(token);
        }
        tokens.add(token); // Добавляем EOF
        
        return tokens;
    }
    
    private Token nextToken() {
        skipWhitespace();
        
        if (pos >= input.length()) {
            return new Token(Token.Type.EOF, "", line, column);
        }
        
        char ch = input.charAt(pos);
        int startLine = line;
        int startColumn = column;
        
        // Операторы и знаки препинания
        switch (ch) {
            case '=':
                pos++;
                column++;
                return new Token(Token.Type.EQ, "=", startLine, startColumn);
            case '<':
                pos++;
                column++;
                if (pos < input.length() && input.charAt(pos) == '=') {
                    pos++;
                    column++;
                    return new Token(Token.Type.LE, "<=", startLine, startColumn);
                } else if (pos < input.length() && input.charAt(pos) == '>') {
                    pos++;
                    column++;
                    return new Token(Token.Type.NE, "<>", startLine, startColumn);
                }
                return new Token(Token.Type.LT, "<", startLine, startColumn);
            case '>':
                pos++;
                column++;
                if (pos < input.length() && input.charAt(pos) == '=') {
                    pos++;
                    column++;
                    return new Token(Token.Type.GE, ">=", startLine, startColumn);
                }
                return new Token(Token.Type.GT, ">", startLine, startColumn);
            case ',':
                pos++;
                column++;
                return new Token(Token.Type.COMMA, ",", startLine, startColumn);
            case ';':
                pos++;
                column++;
                return new Token(Token.Type.SEMICOLON, ";", startLine, startColumn);
            case '(':
                pos++;
                column++;
                return new Token(Token.Type.LPAREN, "(", startLine, startColumn);
            case ')':
                pos++;
                column++;
                return new Token(Token.Type.RPAREN, ")", startLine, startColumn);
            case '.':
                pos++;
                column++;
                return new Token(Token.Type.DOT, ".", startLine, startColumn);
            case '*':
                pos++;
                column++;
                return new Token(Token.Type.STAR, "*", startLine, startColumn);
            case '\'':
                return readString(startLine, startColumn);
        }
        
        // Числа
        if (Character.isDigit(ch)) {
            return readNumber(startLine, startColumn);
        }
        
        // Идентификаторы и ключевые слова
        if (Character.isLetter(ch) || ch == '_') {
            return readIdentifierOrKeyword(startLine, startColumn);
        }
        
        pos++;
        column++;
        return new Token(Token.Type.UNKNOWN, String.valueOf(ch), startLine, startColumn);
    }
    
    private void skipWhitespace() {
        while (pos < input.length()) {
            char ch = input.charAt(pos);
            if (ch == ' ' || ch == '\t') {
                pos++;
                column++;
            } else if (ch == '\n') {
                pos++;
                line++;
                column = 1;
            } else if (ch == '\r') {
                pos++;
                if (pos < input.length() && input.charAt(pos) == '\n') {
                    pos++;
                }
                line++;
                column = 1;
            } else if (ch == '-' && pos + 1 < input.length() && input.charAt(pos + 1) == '-') {
                // SQL комментарий -- до конца строки
                while (pos < input.length() && input.charAt(pos) != '\n' && input.charAt(pos) != '\r') {
                    pos++;
                }
            } else {
                break;
            }
        }
    }
    
    private Token readString(int startLine, int startColumn) {
        pos++; // Пропускаем открывающую кавычку
        column++;
        StringBuilder sb = new StringBuilder();
        
        while (pos < input.length()) {
            char ch = input.charAt(pos);
            if (ch == '\'') {
                pos++;
                column++;
                return new Token(Token.Type.STRING, sb.toString(), startLine, startColumn);
            } else if (ch == '\n') {
                line++;
                column = 1;
            }
            sb.append(ch);
            pos++;
            column++;
        }
        
        return new Token(Token.Type.STRING, sb.toString(), startLine, startColumn);
    }
    
    private Token readNumber(int startLine, int startColumn) {
        StringBuilder sb = new StringBuilder();
        
        while (pos < input.length() && Character.isDigit(input.charAt(pos))) {
            sb.append(input.charAt(pos));
            pos++;
            column++;
        }
        
        return new Token(Token.Type.NUMBER, sb.toString(), startLine, startColumn);
    }
    
    private Token readIdentifierOrKeyword(int startLine, int startColumn) {
        StringBuilder sb = new StringBuilder();
        
        while (pos < input.length() && 
               (Character.isLetterOrDigit(input.charAt(pos)) || 
                input.charAt(pos) == '_')) {
            sb.append(input.charAt(pos));
            pos++;
            column++;
        }
        
        String originalValue = sb.toString();
        String upperValue = originalValue.toUpperCase();
        Token.Type keywordType = getKeywordType(upperValue);
        
        if (keywordType != null) {
            return new Token(keywordType, upperValue, startLine, startColumn);
        }
        
        // Для идентификаторов сохраняем оригинальное значение
        return new Token(Token.Type.IDENTIFIER, originalValue, startLine, startColumn);
    }
    
    private Token.Type getKeywordType(String keyword) {
        switch (keyword) {
            case "CREATE": return Token.Type.CREATE;
            case "TABLE": return Token.Type.TABLE;
            case "DROP": return Token.Type.DROP;
            case "INSERT": return Token.Type.INSERT;
            case "INTO": return Token.Type.INTO;
            case "VALUES": return Token.Type.VALUES;
            case "SELECT": return Token.Type.SELECT;
            case "FROM": return Token.Type.FROM;
            case "WHERE": return Token.Type.WHERE;
            case "AND": return Token.Type.AND;
            case "OR": return Token.Type.OR;
            case "NOT": return Token.Type.NOT;
            case "AS": return Token.Type.AS;
            case "INDEX": return Token.Type.INDEX;
            case "ON": return Token.Type.ON;
            default: return null;
        }
    }
}
