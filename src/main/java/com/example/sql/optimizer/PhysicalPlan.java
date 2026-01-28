package com.example.sql.optimizer;

import java.util.ArrayList;
import java.util.List;

/**
 * Физический план выполнения запроса
 */
public class PhysicalPlan {
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
    private PhysicalOperator rootOperator;
    
    public PhysicalPlan(Type type) {
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
    
    public PhysicalOperator getRootOperator() {
        return rootOperator;
    }
    
    public void setRootOperator(PhysicalOperator rootOperator) {
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
    
    public abstract static class PhysicalOperator {
        protected List<PhysicalOperator> children;
        protected String operatorType;
        
        public PhysicalOperator(String operatorType) {
            this.operatorType = operatorType;
            this.children = new ArrayList<>();
        }
        
        public void addChild(PhysicalOperator child) {
            children.add(child);
        }
        
        public List<PhysicalOperator> getChildren() {
            return children;
        }
        
        public String getOperatorType() {
            return operatorType;
        }
    }
    
    public static class SeqScanOperator extends PhysicalOperator {
        private String tableName;
        
        public SeqScanOperator(String tableName) {
            super("SeqScan");
            this.tableName = tableName;
        }
        
        public String getTableName() {
            return tableName;
        }
    }
    
    public static class IndexScanOperator extends PhysicalOperator {
        private String tableName;
        private String indexName;
        private String columnName;
        private Object value;
        private boolean isRangeScan;
        private Object rangeStart;
        private Object rangeEnd;
        
        public IndexScanOperator(String tableName, String indexName, 
                                String columnName, Object value) {
            super("IndexScan");
            this.tableName = tableName;
            this.indexName = indexName;
            this.columnName = columnName;
            this.value = value;
            this.isRangeScan = false;
        }
        
        public IndexScanOperator(String tableName, String indexName,
                                String columnName, Object rangeStart, Object rangeEnd) {
            super("IndexScan");
            this.tableName = tableName;
            this.indexName = indexName;
            this.columnName = columnName;
            this.rangeStart = rangeStart;
            this.rangeEnd = rangeEnd;
            this.isRangeScan = true;
        }
        
        public String getTableName() {
            return tableName;
        }
        
        public String getIndexName() {
            return indexName;
        }
        
        public String getColumnName() {
            return columnName;
        }
        
        public Object getValue() {
            return value;
        }
        
        public boolean isRangeScan() {
            return isRangeScan;
        }
        
        public Object getRangeStart() {
            return rangeStart;
        }
        
        public Object getRangeEnd() {
            return rangeEnd;
        }
    }
    
    public static class FilterOperator extends PhysicalOperator {
        private String columnName;
        private String operator;
        private Object value;
        
        public FilterOperator(String columnName, String operator, Object value) {
            super("Filter");
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
    
    public static class ProjectOperator extends PhysicalOperator {
        private List<String> columns;
        
        public ProjectOperator(List<String> columns) {
            super("Project");
            this.columns = columns;
        }
        
        public List<String> getColumns() {
            return columns;
        }
    }
}

