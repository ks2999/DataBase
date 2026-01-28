package com.example.storage;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Файл таблицы - управление страницами таблицы
 */
public class TableFile {
    private String tableName;
    private Path filePath;
    private List<Integer> pageIds;
    private int nextPageId;
    
    public TableFile(String tableName, String dataDir) {
        this.tableName = tableName;
        this.filePath = Paths.get(dataDir, tableName + ".tbl");
        this.pageIds = new ArrayList<>();
        this.nextPageId = 0;
        loadMetadata();
    }
    
    private void loadMetadata() {
        Path metaPath = Paths.get(filePath.getParent().toString(), tableName + ".meta");
        if (Files.exists(metaPath)) {
            try (ObjectInputStream ois = new ObjectInputStream(
                    new FileInputStream(metaPath.toFile()))) {
                @SuppressWarnings("unchecked")
                List<Integer> loadedPageIds = (List<Integer>) ois.readObject();
                this.pageIds = loadedPageIds;
                this.nextPageId = ois.readInt();
            } catch (Exception e) {
                // Если не удалось загрузить, начинаем с нуля
                this.pageIds = new ArrayList<>();
                this.nextPageId = 0;
            }
        }
    }
    
    public void saveMetadata() {
        try {
            Files.createDirectories(filePath.getParent());
            Path metaPath = Paths.get(filePath.getParent().toString(), tableName + ".meta");
            try (ObjectOutputStream oos = new ObjectOutputStream(
                    new FileOutputStream(metaPath.toFile()))) {
                oos.writeObject(pageIds);
                oos.writeInt(nextPageId);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to save metadata", e);
        }
    }
    
    public int allocatePage() {
        int pageId = nextPageId++;
        pageIds.add(pageId);
        saveMetadata();
        return pageId;
    }
    
    public List<Integer> getPageIds() {
        return new ArrayList<>(pageIds);
    }
    
    public Path getFilePath() {
        return filePath;
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public void savePage(Page page) {
        try {
            Files.createDirectories(filePath.getParent());
            Path pagePath = Paths.get(filePath.getParent().toString(), 
                    tableName + "_page_" + page.getPageId() + ".dat");
            try (ObjectOutputStream oos = new ObjectOutputStream(
                    new FileOutputStream(pagePath.toFile()))) {
                oos.writeObject(page);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to save page", e);
        }
    }
    
    public Page loadPage(int pageId) {
        try {
            Path pagePath = Paths.get(filePath.getParent().toString(), 
                    tableName + "_page_" + pageId + ".dat");
            if (!Files.exists(pagePath)) {
                return new Page(pageId);
            }
            try (ObjectInputStream ois = new ObjectInputStream(
                    new FileInputStream(pagePath.toFile()))) {
                return (Page) ois.readObject();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load page", e);
        }
    }
}

