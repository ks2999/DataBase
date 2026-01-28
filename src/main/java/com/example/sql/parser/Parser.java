package com.example.sql.parser;

import com.example.sql.lexer.Token;

import java.util.List;

/**
 * Парсер SQL - построение AST из токенов
 */
public class Parser {
    private List<Token> tokens;
    private int pos;
    
    public Parser(List<Token> tokens) {
        this.tokens = tokens;
        this.pos = 0;
    }
    
    public ASTNode parse() {
        if (tokens.isEmpty()) {
            throw new RuntimeException("Empty query");
        }
        
        Token token = current();
        
        switch (token.getType()) {
            case CREATE:
                return parseCreate();
            case DROP:
                return parseDrop();
            case INSERT:
                return parseInsert();
            case SELECT:
                return parseSelect();
            default:
                throw new RuntimeException("Unexpected token: " + token.getType() + " (" + token.getValue() + ")");
        }
    }
    
    private ASTNode parseCreate() {
        // CREATE уже прочитан в parse()
        advance();
        
        Token next = current();
        if (next.getType() == Token.Type.TABLE) {
            return parseCreateTable();
        } else if (next.getType() == Token.Type.INDEX) {
            return parseCreateIndex();
        } else {
            throw new RuntimeException("Expected TABLE or INDEX after CREATE, got: " + next.getType());
        }
    }
    
    private ASTNode parseDrop() {
        // DROP уже прочитан в parse()
        advance();
        expect(Token.Type.TABLE);
        
        Token tableNameToken = expect(Token.Type.IDENTIFIER);
        String tableName = tableNameToken.getValue();
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new RuntimeException("Table name cannot be empty");
        }
        
        ASTNode dropNode = new ASTNode(ASTNode.Type.DROP_TABLE, tableName);
        return dropNode;
    }
    
    private ASTNode parseCreateTable() {
        expect(Token.Type.TABLE);
        
        Token tableNameToken = expect(Token.Type.IDENTIFIER);
        String tableName = tableNameToken.getValue();
        if (tableName == null || tableName.isEmpty()) {
            throw new RuntimeException("Table name cannot be empty");
        }
        
        ASTNode createNode = new ASTNode(ASTNode.Type.CREATE_TABLE, tableName);
        
        expect(Token.Type.LPAREN);
        
        // Парсим колонки
        boolean first = true;
        while (current().getType() != Token.Type.RPAREN) {
            if (!first) {
                expect(Token.Type.COMMA);
            }
            first = false;
            
            ASTNode colDef = parseColumnDef();
            createNode.addChild(colDef);
        }
        
        expect(Token.Type.RPAREN);
        return createNode;
    }
    
    private ASTNode parseCreateIndex() {
        expect(Token.Type.INDEX);
        
        Token indexNameToken = expect(Token.Type.IDENTIFIER);
        String indexName = indexNameToken != null ? indexNameToken.getValue() : null;
        if (indexName == null || indexName.isEmpty()) {
            throw new RuntimeException("Index name cannot be empty");
        }
        
        expect(Token.Type.ON);
        
        Token tableNameToken = expect(Token.Type.IDENTIFIER);
        String tableName = tableNameToken != null ? tableNameToken.getValue() : null;
        if (tableName == null || tableName.isEmpty()) {
            throw new RuntimeException("Table name cannot be empty");
        }
        
        expect(Token.Type.LPAREN);
        
        Token columnNameToken = expect(Token.Type.IDENTIFIER);
        String columnName = columnNameToken != null ? columnNameToken.getValue() : null;
        if (columnName == null || columnName.isEmpty()) {
            throw new RuntimeException("Column name cannot be empty");
        }
        
        expect(Token.Type.RPAREN);
        
        ASTNode createIndexNode = new ASTNode(ASTNode.Type.CREATE_TABLE);
        createIndexNode.setValue("INDEX:" + indexName + ":" + tableName + ":" + columnName);
        return createIndexNode;
    }
    
    private ASTNode parseColumnDef() {
        Token colNameToken = expect(Token.Type.IDENTIFIER);
        String colName = colNameToken.getValue();
        
        Token colTypeToken = expect(Token.Type.IDENTIFIER);
        String colType = colTypeToken.getValue();
        
        ASTNode colDef = new ASTNode(ASTNode.Type.COLUMN_DEF);
        colDef.addChild(new ASTNode(ASTNode.Type.IDENTIFIER, colName));
        colDef.addChild(new ASTNode(ASTNode.Type.IDENTIFIER, colType));
        
        return colDef;
    }
    
    private ASTNode parseInsert() {
        // INSERT уже прочитан в parse()
        advance();
        expect(Token.Type.INTO);
        
        Token tableNameToken = expect(Token.Type.IDENTIFIER);
        String tableName = tableNameToken.getValue();
        if (tableName == null || tableName.isEmpty()) {
            throw new RuntimeException("Table name cannot be empty");
        }
        
        ASTNode insertNode = new ASTNode(ASTNode.Type.INSERT, tableName);
        
        // Опциональный список колонок
        if (current().getType() == Token.Type.LPAREN) {
            advance(); // Пропускаем LPAREN
            ASTNode colList = new ASTNode(ASTNode.Type.COLUMN_LIST);
            
            boolean first = true;
            while (current().getType() != Token.Type.RPAREN) {
                if (!first) {
                    expect(Token.Type.COMMA);
                }
                first = false;
                
                Token colNameToken = expect(Token.Type.IDENTIFIER);
                colList.addChild(new ASTNode(ASTNode.Type.IDENTIFIER, colNameToken.getValue()));
            }
            
            expect(Token.Type.RPAREN);
            insertNode.addChild(colList);
        }
        
        expect(Token.Type.VALUES);
        expect(Token.Type.LPAREN);
        
        ASTNode valuesList = new ASTNode(ASTNode.Type.COLUMN_LIST);
        boolean first = true;
        while (current().getType() != Token.Type.RPAREN) {
            if (!first) {
                expect(Token.Type.COMMA);
            }
            first = false;
            
            ASTNode expr = parseExpression();
            valuesList.addChild(expr);
        }
        
        expect(Token.Type.RPAREN);
        insertNode.addChild(valuesList);
        
        return insertNode;
    }
    
    private ASTNode parseSelect() {
        // SELECT уже прочитан в parse()
        advance();
        
        ASTNode selectNode = new ASTNode(ASTNode.Type.SELECT);
        
        // Список колонок
        ASTNode colList = new ASTNode(ASTNode.Type.COLUMN_LIST);
        if (current().getType() == Token.Type.STAR) {
            advance();
            colList.addChild(new ASTNode(ASTNode.Type.IDENTIFIER, "*"));
        } else {
            boolean first = true;
            while (current().getType() != Token.Type.FROM) {
                if (!first) {
                    expect(Token.Type.COMMA);
                }
                first = false;
                
                Token colNameToken = expect(Token.Type.IDENTIFIER);
                colList.addChild(new ASTNode(ASTNode.Type.IDENTIFIER, colNameToken.getValue()));
            }
        }
        selectNode.addChild(colList);
        
        expect(Token.Type.FROM);
        
        Token tableNameToken = expect(Token.Type.IDENTIFIER);
        String tableName = tableNameToken.getValue();
        if (tableName == null || tableName.isEmpty()) {
            throw new RuntimeException("Table name cannot be empty");
        }
        selectNode.addChild(new ASTNode(ASTNode.Type.IDENTIFIER, tableName));
        
        // Опциональное WHERE условие
        if (current().getType() == Token.Type.WHERE) {
            advance();
            ASTNode whereClause = parseWhereClause();
            selectNode.addChild(whereClause);
        }
        
        return selectNode;
    }
    
    private ASTNode parseWhereClause() {
        ASTNode whereNode = new ASTNode(ASTNode.Type.WHERE_CLAUSE);
        ASTNode expr = parseExpression();
        whereNode.addChild(expr);
        return whereNode;
    }
    
    private ASTNode parseExpression() {
        ASTNode left = parseTerm();
        
        while (current().getType() == Token.Type.AND || 
               current().getType() == Token.Type.OR) {
            Token op = advance();
            ASTNode right = parseTerm();
            ASTNode binOp = new ASTNode(ASTNode.Type.BINARY_OP, op.getValue());
            binOp.addChild(left);
            binOp.addChild(right);
            left = binOp;
        }
        
        return left;
    }
    
    private ASTNode parseTerm() {
        ASTNode left = parseFactor();
        
        while (current().getType() == Token.Type.EQ ||
               current().getType() == Token.Type.NE ||
               current().getType() == Token.Type.LT ||
               current().getType() == Token.Type.LE ||
               current().getType() == Token.Type.GT ||
               current().getType() == Token.Type.GE) {
            Token op = current(); // Получаем токен оператора
            advance(); // Продвигаем позицию
            ASTNode right = parseFactor();
            
            // Проверяем, что оператор правильный
            String opValue = op.getValue();
            if (opValue == null) {
                throw new RuntimeException("Operator token value is null");
            }
            
            ASTNode binOp = new ASTNode(ASTNode.Type.BINARY_OP, opValue);
            binOp.addChild(left);
            binOp.addChild(right);
            left = binOp;
        }
        
        return left;
    }
    
    private ASTNode parseFactor() {
        Token token = current();
        
        if (token.getType() == Token.Type.IDENTIFIER) {
            advance();
            return new ASTNode(ASTNode.Type.IDENTIFIER, token.getValue());
        } else if (token.getType() == Token.Type.STRING) {
            advance();
            return new ASTNode(ASTNode.Type.LITERAL, token.getValue());
        } else if (token.getType() == Token.Type.NUMBER) {
            advance();
            return new ASTNode(ASTNode.Type.LITERAL, token.getValue());
        } else if (token.getType() == Token.Type.LPAREN) {
            advance();
            ASTNode expr = parseExpression();
            expect(Token.Type.RPAREN);
            return expr;
        }
        
        throw new RuntimeException("Unexpected token in factor: " + token.getType() + " (" + token.getValue() + ")");
    }
    
    private Token current() {
        if (pos >= tokens.size()) {
            return tokens.get(tokens.size() - 1); // EOF
        }
        return tokens.get(pos);
    }
    
    private Token advance() {
        if (pos < tokens.size()) {
            pos++;
        }
        return current();
    }
    
    private Token expect(Token.Type type) {
        Token token = current();
        if (token.getType() != type) {
            throw new RuntimeException("Expected " + type + ", got " + token.getType() + 
                    " (" + token.getValue() + ") at line " + token.getLine() + ":" + token.getColumn());
        }
        Token result = token; // Сохраняем токен ДО продвижения
        advance(); // Продвигаем позицию
        return result; // Возвращаем сохраненный токен
    }
}
