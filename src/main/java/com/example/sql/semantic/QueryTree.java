package com.example.sql.semantic;

import java.util.ArrayList;
import java.util.List;

/**
 * QueryTree - семантическое представление запроса
 */
public class QueryTree {
    public enum Type {
        CREATE_TABLE,
        CREATE_INDEX,
        DROP_TABLE,
        INSERT,
        SELECT
    }
    
    private Type type;
    private String tableName;
    private List<ColumnDef> columns;
    private List<String> selectColumns;
    private List<Object> insertValues;
    private Expression whereCondition;
    
    public QueryTree(Type type) {
        this.type = type;
        this.columns = new ArrayList<>();
        this.selectColumns = new ArrayList<>();
        this.insertValues = new ArrayList<>();
    }
    
    public Type getType() {
        return type;
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    public List<ColumnDef> getColumns() {
        return columns;
    }
    
    public List<String> getSelectColumns() {
        return selectColumns;
    }
    
    public List<Object> getInsertValues() {
        return insertValues;
    }
    
    public Expression getWhereCondition() {
        return whereCondition;
    }
    
    public void setWhereCondition(Expression whereCondition) {
        this.whereCondition = whereCondition;
    }
    
    public static class ColumnDef {
        private String name;
        private String type;
        
        public ColumnDef(String name, String type) {
            this.name = name;
            this.type = type;
        }
        
        public String getName() {
            return name;
        }
        
        public String getType() {
            return type;
        }
    }
    
    public static class Expression {
        public enum OpType {
            EQ, NE, LT, LE, GT, GE, AND, OR
        }
        
        private OpType opType;
        private String columnName;
        private Object value;
        private Expression left;
        private Expression right;
        
        public Expression(OpType opType, String columnName, Object value) {
            this.opType = opType;
            this.columnName = columnName;
            this.value = value;
        }
        
        public Expression(OpType opType, Expression left, Expression right) {
            this.opType = opType;
            this.left = left;
            this.right = right;
        }
        
        public OpType getOpType() {
            return opType;
        }
        
        public String getColumnName() {
            return columnName;
        }
        
        public Object getValue() {
            return value;
        }
        
        public Expression getLeft() {
            return left;
        }
        
        public Expression getRight() {
            return right;
        }
        
        public boolean isBinary() {
            return left != null && right != null;
        }
    }
}

