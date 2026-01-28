package com.example.sql.parser;

import java.util.ArrayList;
import java.util.List;

/**
 * Узел AST (Abstract Syntax Tree)
 */
public class ASTNode {
    public enum Type {
        CREATE_TABLE,
        DROP_TABLE,
        INSERT,
        SELECT,
        COLUMN_DEF,
        EXPRESSION,
        BINARY_OP,
        LITERAL,
        IDENTIFIER,
        WHERE_CLAUSE,
        COLUMN_LIST
    }
    
    private Type type;
    private String value;
    private List<ASTNode> children;
    
    public ASTNode(Type type) {
        this.type = type;
        this.children = new ArrayList<>();
    }
    
    public ASTNode(Type type, String value) {
        this.type = type;
        this.value = value;
        this.children = new ArrayList<>();
    }
    
    public Type getType() {
        return type;
    }
    
    public String getValue() {
        return value;
    }
    
    public void setValue(String value) {
        this.value = value;
    }
    
    public List<ASTNode> getChildren() {
        return children;
    }
    
    public void addChild(ASTNode child) {
        children.add(child);
    }
    
    @Override
    public String toString() {
        return toString(0);
    }
    
    private String toString(int indent) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < indent; i++) {
            sb.append("  ");
        }
        sb.append(type);
        if (value != null) {
            sb.append(": ").append(value);
        }
        sb.append("\n");
        for (ASTNode child : children) {
            sb.append(((ASTNode) child).toString(indent + 1));
        }
        return sb.toString();
    }
}

