package com.example.executor;

import com.example.storage.TableMetadata;

import java.util.ArrayList;
import java.util.List;

/**
 * Project executor - проекция колонок
 */
public class ProjectExecutor implements Executor {
    private Executor child;
    private List<String> columns;
    private List<Integer> columnIndices;
    private boolean isOpen;
    
    public ProjectExecutor(Executor child, List<String> columns, 
                          TableMetadata metadata) {
        this.child = child;
        this.columns = columns;
        this.columnIndices = new ArrayList<>();
        
        // Определяем индексы колонок
        for (String colName : columns) {
            int index = metadata.getColumnIndex(colName);
            if (index == -1) {
                throw new RuntimeException("Column not found: " + colName);
            }
            columnIndices.add(index);
        }
    }
    
    @Override
    public void open() {
        child.open();
        isOpen = true;
    }
    
    @Override
    public Row next() {
        if (!isOpen) {
            return null;
        }
        
        Row inputRow = child.next();
        if (inputRow == null) {
            return null;
        }
        
        // Создаем новую строку только с нужными колонками
        Row outputRow = new Row();
        for (int index : columnIndices) {
            outputRow.addValue(inputRow.getValue(index));
        }
        
        return outputRow;
    }
    
    @Override
    public void close() {
        child.close();
        isOpen = false;
    }
}


