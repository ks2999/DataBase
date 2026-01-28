package com.example.server;

import java.util.ArrayList;
import java.util.List;

/**
 * Формализованный wire-протокол для клиент-серверного взаимодействия
 */
public class Protocol {
    public static final String VERSION = "1.0";
    public static final String DELIMITER = "\n---END---\n";
    
    /**
     * Сериализация запроса
     */
    public static String serializeRequest(String sql) {
        return "QUERY:" + sql + DELIMITER;
    }
    
    /**
     * Десериализация запроса
     */
    public static String deserializeRequest(String data) {
        if (data.startsWith("QUERY:")) {
            return data.substring(6, data.indexOf(DELIMITER));
        }
        throw new RuntimeException("Invalid request format");
    }
    
    /**
     * Сериализация ответа
     */
    public static String serializeResponse(boolean success, String message, 
                                          List<Row> rows, List<String> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append(success ? "OK" : "ERROR").append("\n");
        sb.append("MSG:").append(message).append("\n");
        
        if (success && rows != null && !rows.isEmpty()) {
            sb.append("ROWS:").append(rows.size()).append("\n");
            if (columns != null && !columns.isEmpty()) {
                sb.append("COLS:").append(String.join(",", columns)).append("\n");
            }
            sb.append("DATA:\n");
            for (Row row : rows) {
                List<String> values = new ArrayList<>();
                for (int i = 0; i < row.size(); i++) {
                    Object val = row.getValue(i);
                    values.add(val != null ? val.toString() : "NULL");
                }
                sb.append(String.join("|", values)).append("\n");
            }
        }
        sb.append(DELIMITER);
        return sb.toString();
    }
    
    /**
     * Десериализация ответа
     */
    public static Response deserializeResponse(String data) {
        String[] lines = data.split("\n");
        boolean success = lines[0].equals("OK");
        String message = "";
        List<String> columns = new ArrayList<>();
        List<Row> rows = new ArrayList<>();
        
        int i = 1;
        while (i < lines.length && !lines[i].equals("---END---")) {
            if (lines[i].startsWith("MSG:")) {
                message = lines[i].substring(4);
            } else if (lines[i].startsWith("COLS:")) {
                String cols = lines[i].substring(5);
                for (String col : cols.split(",")) {
                    columns.add(col);
                }
            } else if (lines[i].startsWith("ROWS:")) {
                // Количество строк
            } else if (lines[i].equals("DATA:")) {
                i++;
                while (i < lines.length && !lines[i].equals("---END---")) {
                    String[] values = lines[i].split("\\|");
                    Row row = new Row();
                    for (String val : values) {
                        row.addValue(val.equals("NULL") ? null : val);
                    }
                    rows.add(row);
                    i++;
                }
                break;
            }
            i++;
        }
        
        return new Response(success, message, rows, columns);
    }
    
    public static class Row {
        private List<Object> values = new ArrayList<>();
        
        public void addValue(Object value) {
            values.add(value);
        }
        
        public Object getValue(int index) {
            return values.get(index);
        }
        
        public int size() {
            return values.size();
        }
    }
    
    public static class Response {
        private boolean success;
        private String message;
        private List<Row> rows;
        private List<String> columns;
        
        public Response(boolean success, String message, List<Row> rows, List<String> columns) {
            this.success = success;
            this.message = message;
            this.rows = rows;
            this.columns = columns;
        }
        
        public boolean isSuccess() {
            return success;
        }
        
        public String getMessage() {
            return message;
        }
        
        public List<Row> getRows() {
            return rows;
        }
        
        public List<String> getColumns() {
            return columns;
        }
    }
}

