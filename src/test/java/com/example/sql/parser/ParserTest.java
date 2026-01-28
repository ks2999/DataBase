package com.example.sql.parser;

import com.example.sql.lexer.Lexer;
import com.example.sql.lexer.Token;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

public class ParserTest {
    
    @Test
    public void testParseCreateTable() {
        Lexer lexer = new Lexer("CREATE TABLE users (id INTEGER, name VARCHAR)");
        List<Token> tokens = lexer.tokenize();
        Parser parser = new Parser(tokens);
        
        ASTNode ast = parser.parse();
        
        assertEquals(ASTNode.Type.CREATE_TABLE, ast.getType());
        assertEquals("users", ast.getValue());
        assertEquals(2, ast.getChildren().size());
    }
    
    @Test
    public void testParseInsert() {
        Lexer lexer = new Lexer("INSERT INTO users VALUES (1, 'Alice', 25)");
        List<Token> tokens = lexer.tokenize();
        Parser parser = new Parser(tokens);
        
        ASTNode ast = parser.parse();
        
        assertEquals(ASTNode.Type.INSERT, ast.getType());
        assertEquals("users", ast.getValue());
        assertEquals(1, ast.getChildren().size()); // values list
    }
    
    @Test
    public void testParseSelect() {
        Lexer lexer = new Lexer("SELECT * FROM users");
        List<Token> tokens = lexer.tokenize();
        Parser parser = new Parser(tokens);
        
        ASTNode ast = parser.parse();
        
        assertEquals(ASTNode.Type.SELECT, ast.getType());
        assertEquals(2, ast.getChildren().size()); // column list and table name
    }
    
    @Test
    public void testParseSelectWhere() {
        Lexer lexer = new Lexer("SELECT * FROM users WHERE age > 25");
        List<Token> tokens = lexer.tokenize();
        Parser parser = new Parser(tokens);
        
        ASTNode ast = parser.parse();
        
        assertEquals(ASTNode.Type.SELECT, ast.getType());
        assertEquals(3, ast.getChildren().size()); // column list, table name, where clause
    }
    
    @Test
    public void testParseCreateIndex() {
        Lexer lexer = new Lexer("CREATE INDEX idx_users_id ON users(id)");
        List<Token> tokens = lexer.tokenize();
        Parser parser = new Parser(tokens);
        
        ASTNode ast = parser.parse();
        
        assertEquals(ASTNode.Type.CREATE_TABLE, ast.getType());
        assertTrue(ast.getValue().startsWith("INDEX:"));
        assertTrue(ast.getValue().contains("idx_users_id"));
        assertTrue(ast.getValue().contains("users"));
        assertTrue(ast.getValue().contains("id"));
    }
}


