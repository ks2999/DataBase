package com.example.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Метаданные таблицы: схема, колонки
 */
public class TableMetadata implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String tableName;
    private List<Column> columns;
    
    public TableMetadata(String tableName) {
        this.tableName = tableName;
        this.columns = new ArrayList<>();
    }
    
    public void addColumn(String name, String type) {
        columns.add(new Column(name, type));
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public List<Column> getColumns() {
        return new ArrayList<>(columns);
    }
    
    public Column getColumn(String name) {
        return columns.stream()
                .filter(c -> c.getName().equalsIgnoreCase(name))
                .findFirst()
                .orElse(null);
    }
    
    public int getColumnIndex(String name) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(name)) {
                return i;
            }
        }
        return -1;
    }
    
    public static class Column implements Serializable {
        private static final long serialVersionUID = 1L;
        private String name;
        private String type;
        
        public Column(String name, String type) {
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
}


