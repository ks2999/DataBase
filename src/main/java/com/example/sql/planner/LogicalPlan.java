package com.example.sql.planner;

import java.util.ArrayList;
import java.util.List;

/**
 * Логический план выполнения запроса
 */
public class LogicalPlan {
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
    private LogicalOperator rootOperator;
    
    public LogicalPlan(Type type) {
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
    
    public LogicalOperator getRootOperator() {
        return rootOperator;
    }
    
    public void setRootOperator(LogicalOperator rootOperator) {
        this.rootOperator = rootOperator;
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
    
    public abstract static class LogicalOperator {
        protected List<LogicalOperator> children;
        
        public LogicalOperator() {
            this.children = new ArrayList<>();
        }
        
        public void addChild(LogicalOperator child) {
            children.add(child);
        }
        
        public List<LogicalOperator> getChildren() {
            return children;
        }
    }
    
    public static class ScanOperator extends LogicalOperator {
        private String tableName;
        
        public ScanOperator(String tableName) {
            this.tableName = tableName;
        }
        
        public String getTableName() {
            return tableName;
        }
    }
    
    public static class FilterOperator extends LogicalOperator {
        private String columnName;
        private String operator;
        private Object value;
        
        public FilterOperator(String columnName, String operator, Object value) {
            this.columnName = columnName;
            this.operator = operator;
            this.value = value;
        }
        
        public String getColumnName() {
            return columnName;
        }
        
        public String getOperator() {
            return operator;
        }
        
        public Object getValue() {
            return value;
        }
    }
    
    public static class ProjectOperator extends LogicalOperator {
        private List<String> columns;
        
        public ProjectOperator(List<String> columns) {
            this.columns = columns;
        }
        
        public List<String> getColumns() {
            return columns;
        }
    }
}

