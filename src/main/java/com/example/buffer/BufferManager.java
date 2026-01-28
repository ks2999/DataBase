package com.example.buffer;

import com.example.storage.Page;
import com.example.storage.TableFile;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Буферный менеджер - управление страницами в памяти
 */
public class BufferManager {
    private final int poolSize;
    private Map<String, Page> bufferPool;
    private Map<String, Integer> accessOrder;
    private int accessCounter;
    
    public BufferManager(int poolSize) {
        this.poolSize = poolSize;
        this.bufferPool = new LinkedHashMap<String, Page>(poolSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Page> eldest) {
                if (size() > poolSize) {
                    Page page = eldest.getValue();
                    if (page.isDirty()) {
                        flushPage(eldest.getKey(), page);
                    }
                    return true;
                }
                return false;
            }
        };
        this.accessOrder = new HashMap<>();
        this.accessCounter = 0;
    }
    
    public Page getPage(TableFile tableFile, int pageId) {
        String key = tableFile.getTableName() + "_" + pageId;
        
        // Проверяем в буфере
        Page page = bufferPool.get(key);
        if (page != null) {
            accessOrder.put(key, accessCounter++);
            return page;
        }
        
        // Загружаем с диска
        page = tableFile.loadPage(pageId);
        
        // Добавляем в буфер (LRU автоматически удалит старые)
        if (bufferPool.size() >= poolSize) {
            evictLRU();
        }
        
        bufferPool.put(key, page);
        accessOrder.put(key, accessCounter++);
        return page;
    }
    
    public void pinPage(TableFile tableFile, int pageId) {
        String key = tableFile.getTableName() + "_" + pageId;
        Page page = bufferPool.get(key);
        if (page != null) {
            accessOrder.put(key, accessCounter++);
        } else {
            // Если страницы нет в буфере, добавляем её
            page = tableFile.loadPage(pageId);
            if (bufferPool.size() >= poolSize) {
                evictLRU();
            }
            bufferPool.put(key, page);
            accessOrder.put(key, accessCounter++);
        }
    }
    
    public void addPage(TableFile tableFile, Page page) {
        String key = tableFile.getTableName() + "_" + page.getPageId();
        if (bufferPool.size() >= poolSize) {
            evictLRU();
        }
        bufferPool.put(key, page);
        accessOrder.put(key, accessCounter++);
    }
    
    public void markDirty(TableFile tableFile, int pageId) {
        String key = tableFile.getTableName() + "_" + pageId;
        Page page = bufferPool.get(key);
        if (page != null) {
            page.setDirty(true);
        }
    }
    
    public void flushPage(String key, Page page) {
        // Извлекаем tableName и pageId из key
        String[] parts = key.split("_");
        if (parts.length >= 2) {
            String tableName = parts[0];
            // pageId используется для логирования, но не критичен
            TableFile tableFile = new TableFile(tableName, "data");
            tableFile.savePage(page);
            page.setDirty(false);
        }
    }
    
    private void evictLRU() {
        String lruKey = null;
        int minAccess = Integer.MAX_VALUE;
        
        for (Map.Entry<String, Integer> entry : accessOrder.entrySet()) {
            if (entry.getValue() < minAccess) {
                minAccess = entry.getValue();
                lruKey = entry.getKey();
            }
        }
        
        if (lruKey != null) {
            Page page = bufferPool.get(lruKey);
            if (page != null && page.isDirty()) {
                flushPage(lruKey, page);
            }
            bufferPool.remove(lruKey);
            accessOrder.remove(lruKey);
        }
    }
    
    public void flushAll() {
        for (Map.Entry<String, Page> entry : bufferPool.entrySet()) {
            if (entry.getValue().isDirty()) {
                flushPage(entry.getKey(), entry.getValue());
            }
        }
    }
}

