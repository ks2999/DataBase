package com.example.executor;

import java.util.ArrayList;
import java.util.List;

/**
 * Строка данных
 */
public class Row {
    private List<Object> values;
    
    public Row() {
        this.values = new ArrayList<>();
    }
    
    public Row(List<Object> values) {
        this.values = new ArrayList<>(values);
    }
    
    public void addValue(Object value) {
        values.add(value);
    }
    
    public Object getValue(int index) {
        return values.get(index);
    }
    
    public List<Object> getValues() {
        return new ArrayList<>(values);
    }
    
    public int size() {
        return values.size();
    }
    
    @Override
    public String toString() {
        return values.toString();
    }
}


