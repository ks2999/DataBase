package com.example.executor;

/**
 * Filter executor - фильтрация строк по условию
 */
public class FilterExecutor implements Executor {
    private Executor child;
    private String columnName;
    private String operator;
    private Object value;
    private int columnIndex;
    private boolean isOpen;
    
    public FilterExecutor(Executor child, String columnName, 
                         String operator, Object value, int columnIndex) {
        this.child = child;
        this.columnName = columnName;
        this.operator = operator;
        this.value = value;
        this.columnIndex = columnIndex;
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
        
        Row row;
        while ((row = child.next()) != null) {
            if (evaluateCondition(row)) {
                return row;
            }
        }
        
        return null;
    }
    
    @Override
    public void close() {
        child.close();
        isOpen = false;
    }
    
    private boolean evaluateCondition(Row row) {
        Object rowValue = row.getValue(columnIndex);
        
        if (rowValue == null) {
            return false;
        }
        
        switch (operator) {
            case "=":
                return compareValues(rowValue, value) == 0;
            case "<>":
                return compareValues(rowValue, value) != 0;
            case "<":
                return compareValues(rowValue, value) < 0;
            case "<=":
                return compareValues(rowValue, value) <= 0;
            case ">":
                return compareValues(rowValue, value) > 0;
            case ">=":
                return compareValues(rowValue, value) >= 0;
            default:
                return false;
        }
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    private int compareValues(Object a, Object b) {
        if (a instanceof Comparable && b instanceof Comparable) {
            return ((Comparable) a).compareTo(b);
        }
        return a.toString().compareTo(b.toString());
    }
}

