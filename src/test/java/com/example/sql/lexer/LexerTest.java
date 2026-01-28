package com.example.sql.lexer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

public class LexerTest {
    
    @Test
    public void testTokenizeCreateTable() {
        Lexer lexer = new Lexer("CREATE TABLE users (id INTEGER, name VARCHAR)");
        List<Token> tokens = lexer.tokenize();
        
        assertEquals(Token.Type.CREATE, tokens.get(0).getType());
        assertEquals(Token.Type.TABLE, tokens.get(1).getType());
        assertEquals(Token.Type.IDENTIFIER, tokens.get(2).getType());
        assertEquals("users", tokens.get(2).getValue());
        assertEquals(Token.Type.LPAREN, tokens.get(3).getType());
    }
    
    @Test
    public void testTokenizeInsert() {
        Lexer lexer = new Lexer("INSERT INTO users VALUES (1, 'Alice', 25)");
        List<Token> tokens = lexer.tokenize();
        
        assertEquals(Token.Type.INSERT, tokens.get(0).getType());
        assertEquals(Token.Type.INTO, tokens.get(1).getType());
        assertEquals(Token.Type.IDENTIFIER, tokens.get(2).getType());
        assertEquals("users", tokens.get(2).getValue());
        assertEquals(Token.Type.VALUES, tokens.get(3).getType());
    }
    
    @Test
    public void testTokenizeSelect() {
        Lexer lexer = new Lexer("SELECT * FROM users WHERE age > 25");
        List<Token> tokens = lexer.tokenize();
        
        assertEquals(Token.Type.SELECT, tokens.get(0).getType());
        assertEquals(Token.Type.STAR, tokens.get(1).getType());
        assertEquals(Token.Type.FROM, tokens.get(2).getType());
        assertEquals(Token.Type.IDENTIFIER, tokens.get(3).getType());
        assertEquals("users", tokens.get(3).getValue());
        assertEquals(Token.Type.WHERE, tokens.get(4).getType());
    }
    
    @Test
    public void testTokenizeString() {
        Lexer lexer = new Lexer("'test string'");
        List<Token> tokens = lexer.tokenize();
        
        assertEquals(Token.Type.STRING, tokens.get(0).getType());
        assertEquals("test string", tokens.get(0).getValue());
    }
    
    @Test
    public void testTokenizeNumber() {
        Lexer lexer = new Lexer("123");
        List<Token> tokens = lexer.tokenize();
        
        assertEquals(Token.Type.NUMBER, tokens.get(0).getType());
        assertEquals("123", tokens.get(0).getValue());
    }
    
    @Test
    public void testTokenizeOperators() {
        Lexer lexer = new Lexer("= <> < <= > >=");
        List<Token> tokens = lexer.tokenize();
        
        assertEquals(Token.Type.EQ, tokens.get(0).getType());
        assertEquals(Token.Type.NE, tokens.get(1).getType());
        assertEquals(Token.Type.LT, tokens.get(2).getType());
        assertEquals(Token.Type.LE, tokens.get(3).getType());
        assertEquals(Token.Type.GT, tokens.get(4).getType());
        assertEquals(Token.Type.GE, tokens.get(5).getType());
    }
    
    @Test
    public void testTokenizeComments() {
        Lexer lexer = new Lexer("CREATE TABLE test -- comment\n(id INTEGER)");
        List<Token> tokens = lexer.tokenize();
        
        assertEquals(Token.Type.CREATE, tokens.get(0).getType());
        assertEquals(Token.Type.TABLE, tokens.get(1).getType());
        assertEquals(Token.Type.IDENTIFIER, tokens.get(2).getType());
        assertEquals("test", tokens.get(2).getValue());
        assertEquals(Token.Type.LPAREN, tokens.get(3).getType());
    }
}


