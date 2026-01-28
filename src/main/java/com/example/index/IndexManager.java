package com.example.index;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Менеджер индексов - управление всеми индексами
 */
public class IndexManager {
    private String dataDir;
    private Map<String, BPlusTree> indexes;
    
    public IndexManager(String dataDir) {
        this.dataDir = dataDir;
        this.indexes = new HashMap<>();
        loadIndexes();
    }
    
    private void loadIndexes() {
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
                if (fileName.endsWith(".idx")) {
                    String indexName = fileName.substring(0, fileName.length() - 4);
                    loadIndexMetadata(indexName);
                }
            });
        } catch (IOException e) {
            // Игнорируем ошибки при загрузке
        }
    }
    
    private void loadIndexMetadata(String indexName) {
        Path metaPath = Paths.get(dataDir, indexName + ".idxmeta");
        if (Files.exists(metaPath)) {
            try (ObjectInputStream ois = new ObjectInputStream(
                    new FileInputStream(metaPath.toFile()))) {
                IndexMetadata meta = (IndexMetadata) ois.readObject();
                BPlusTree tree = new BPlusTree(indexName, meta.tableName, 
                        meta.columnName, dataDir);
                indexes.put(indexName, tree);
            } catch (Exception e) {
                // Игнорируем ошибки
            }
        }
    }
    
    public void createIndex(String indexName, String tableName, String columnName) {
        IndexMetadata meta = new IndexMetadata(tableName, columnName);
        Path metaPath = Paths.get(dataDir, indexName + ".idxmeta");
        try {
            Files.createDirectories(metaPath.getParent());
            try (ObjectOutputStream oos = new ObjectOutputStream(
                    new FileOutputStream(metaPath.toFile()))) {
                oos.writeObject(meta);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to save index metadata", e);
        }
        
        BPlusTree tree = new BPlusTree(indexName, tableName, columnName, dataDir);
        indexes.put(indexName, tree);
    }
    
    public BPlusTree getIndex(String indexName) {
        return indexes.get(indexName);
    }
    
    public BPlusTree findIndexForColumn(String tableName, String columnName) {
        for (Map.Entry<String, BPlusTree> entry : indexes.entrySet()) {
            String indexName = entry.getKey();
            BPlusTree index = entry.getValue();
            
            // Проверяем по метаданным
            Path metaPath = Paths.get(dataDir, indexName + ".idxmeta");
            if (Files.exists(metaPath)) {
                try (ObjectInputStream ois = new ObjectInputStream(
                        new FileInputStream(metaPath.toFile()))) {
                    IndexMetadata meta = (IndexMetadata) ois.readObject();
                    if (meta.tableName.equals(tableName) && 
                        meta.columnName.equalsIgnoreCase(columnName)) {
                        return index;
                    }
                } catch (Exception e) {
                    // Продолжаем поиск
                }
            }
        }
        return null;
    }
    
    private String indexName(BPlusTree tree) {
        for (Map.Entry<String, BPlusTree> entry : indexes.entrySet()) {
            if (entry.getValue() == tree) {
                return entry.getKey();
            }
        }
        return null;
    }
    
    public void saveAll() {
        for (BPlusTree index : indexes.values()) {
            index.saveIndex();
        }
    }
    
    public void dropIndexesForTable(String tableName) {
        // Находим все индексы для таблицы и удаляем их
        java.util.List<String> indexesToRemove = new java.util.ArrayList<>();
        
        for (Map.Entry<String, BPlusTree> entry : indexes.entrySet()) {
            String indexName = entry.getKey();
            Path metaPath = Paths.get(dataDir, indexName + ".idxmeta");
            if (Files.exists(metaPath)) {
                try (ObjectInputStream ois = new ObjectInputStream(
                        new FileInputStream(metaPath.toFile()))) {
                    IndexMetadata meta = (IndexMetadata) ois.readObject();
                    if (meta.tableName.equals(tableName)) {
                        indexesToRemove.add(indexName);
                    }
                } catch (Exception e) {
                    // Игнорируем ошибки
                }
            }
        }
        
        // Удаляем индексы из памяти и с диска
        for (String indexName : indexesToRemove) {
            indexes.remove(indexName);
            
            // Удаляем файлы индекса
            try {
                Path metaPath = Paths.get(dataDir, indexName + ".idxmeta");
                if (Files.exists(metaPath)) {
                    Files.delete(metaPath);
                }
                
                Path idxPath = Paths.get(dataDir, indexName + ".idx");
                if (Files.exists(idxPath)) {
                    Files.delete(idxPath);
                }
            } catch (IOException e) {
                // Игнорируем ошибки удаления файлов
            }
        }
    }
    
    public static class IndexMetadata implements Serializable {
        public String tableName;
        public String columnName;
        
        public IndexMetadata(String tableName, String columnName) {
            this.tableName = tableName;
            this.columnName = columnName;
        }
    }
}

