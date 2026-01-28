package com.example.storage;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Менеджер хранилища - управление таблицами и их метаданными
 */
public class StorageManager {
    private String dataDir;
    private Map<String, TableMetadata> tables;
    private Map<String, TableFile> tableFiles;
    
    public StorageManager(String dataDir) {
        this.dataDir = dataDir;
        this.tables = new HashMap<>();
        this.tableFiles = new HashMap<>();
        loadTables();
    }
    
    private void loadTables() {
        Path dir = Paths.get(dataDir);
        if (!Files.exists(dir)) {
            try {
                Files.createDirectories(dir);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create data directory", e);
            }
            return;
        }
        
        try {
            Files.list(dir).forEach(path -> {
                String fileName = path.getFileName().toString();
                if (fileName.endsWith(".schema")) {
                    String tableName = fileName.substring(0, fileName.length() - 7);
                    loadTableMetadata(tableName);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException("Failed to load tables", e);
        }
    }
    
    private void loadTableMetadata(String tableName) {
        Path schemaPath = Paths.get(dataDir, tableName + ".schema");
        if (Files.exists(schemaPath)) {
            try (ObjectInputStream ois = new ObjectInputStream(
                    new FileInputStream(schemaPath.toFile()))) {
                TableMetadata metadata = (TableMetadata) ois.readObject();
                tables.put(tableName, metadata);
                tableFiles.put(tableName, new TableFile(tableName, dataDir));
            } catch (Exception e) {
                throw new RuntimeException("Failed to load table metadata: " + tableName, e);
            }
        }
    }
    
    public void createTable(TableMetadata metadata) {
        tables.put(metadata.getTableName(), metadata);
        tableFiles.put(metadata.getTableName(), new TableFile(metadata.getTableName(), dataDir));
        saveTableMetadata(metadata);
    }
    
    private void saveTableMetadata(TableMetadata metadata) {
        Path schemaPath = Paths.get(dataDir, metadata.getTableName() + ".schema");
        try {
            Files.createDirectories(schemaPath.getParent());
            try (ObjectOutputStream oos = new ObjectOutputStream(
                    new FileOutputStream(schemaPath.toFile()))) {
                oos.writeObject(metadata);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to save table metadata", e);
        }
    }
    
    public TableMetadata getTableMetadata(String tableName) {
        return tables.get(tableName);
    }
    
    public TableFile getTableFile(String tableName) {
        return tableFiles.get(tableName);
    }
    
    public boolean tableExists(String tableName) {
        return tables.containsKey(tableName);
    }
    
    public void dropTable(String tableName) {
        if (!tables.containsKey(tableName)) {
            throw new RuntimeException("Table does not exist: " + tableName);
        }
        
        // Удаляем из памяти ПЕРЕД удалением файлов
        tables.remove(tableName);
        tableFiles.remove(tableName);
        
        // Удаляем файлы на диске
        try {
            Path dir = Paths.get(dataDir);
            if (Files.exists(dir)) {
                // Удаляем .schema файл
                Path schemaPath = Paths.get(dataDir, tableName + ".schema");
                if (Files.exists(schemaPath)) {
                    Files.delete(schemaPath);
                }
                
                // Удаляем .meta файл
                Path metaPath = Paths.get(dataDir, tableName + ".meta");
                if (Files.exists(metaPath)) {
                    Files.delete(metaPath);
                }
                
                // Удаляем все страницы - собираем список сначала
                java.util.List<Path> pagesToDelete = new java.util.ArrayList<>();
                try (java.util.stream.Stream<Path> stream = Files.list(dir)) {
                    stream.forEach(path -> {
                        String fileName = path.getFileName().toString();
                        if (fileName.startsWith(tableName + "_page_") && fileName.endsWith(".dat")) {
                            pagesToDelete.add(path);
                        }
                    });
                }
                
                // Удаляем страницы
                for (Path pagePath : pagesToDelete) {
                    try {
                        Files.delete(pagePath);
                    } catch (IOException e) {
                        // Игнорируем ошибки удаления отдельных страниц
                    }
                }
                
                // Проверяем, что таблица действительно удалена
                if (Files.exists(schemaPath) || Files.exists(metaPath)) {
                    throw new RuntimeException("Failed to completely delete table files: " + tableName);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete table files: " + tableName, e);
        }
    }
}

