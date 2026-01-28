package com.example.executor;

import com.example.buffer.BufferManager;
import com.example.storage.Page;
import com.example.storage.TableFile;
import com.example.storage.TableMetadata;
import com.example.storage.StorageManager;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * SeqScan executor - последовательное сканирование таблицы
 */
public class SeqScanExecutor implements Executor {
    private StorageManager storageManager;
    private BufferManager bufferManager;
    private String tableName;
    private TableMetadata metadata;
    private TableFile tableFile;
    private List<Integer> pageIds;
    private int currentPageIndex;
    private int currentSlot;
    private Page currentPage;
    private boolean isOpen;
    
    public SeqScanExecutor(StorageManager storageManager, 
                          BufferManager bufferManager, 
                          String tableName) {
        this.storageManager = storageManager;
        this.bufferManager = bufferManager;
        this.tableName = tableName;
    }
    
    @Override
    public void open() {
        this.metadata = storageManager.getTableMetadata(tableName);
        this.tableFile = storageManager.getTableFile(tableName);
        this.pageIds = tableFile.getPageIds();
        this.currentPageIndex = 0;
        this.currentSlot = 0;
        this.isOpen = true;
        
        if (!pageIds.isEmpty()) {
            loadCurrentPage();
        }
    }
    
    @Override
    public Row next() {
        if (!isOpen) {
            return null;
        }
        
        while (currentPageIndex < pageIds.size()) {
            if (currentPage == null) {
                loadCurrentPage();
            }
            
            Row row = readRowFromPage(currentSlot);
            if (row != null) {
                currentSlot++;
                return row;
            }
            
            // Переходим к следующей странице
            currentPageIndex++;
            currentSlot = 0;
            if (currentPageIndex < pageIds.size()) {
                loadCurrentPage();
            } else {
                currentPage = null;
            }
        }
        
        return null;
    }
    
    @Override
    public void close() {
        this.isOpen = false;
        this.currentPage = null;
    }
    
    private void loadCurrentPage() {
        if (currentPageIndex < pageIds.size()) {
            int pageId = pageIds.get(currentPageIndex);
            currentPage = bufferManager.getPage(tableFile, pageId);
        }
    }
    
    private Row readRowFromPage(int slot) {
        if (currentPage == null) {
            return null;
        }
        
        ByteBuffer buffer = currentPage.getBuffer();
        buffer.rewind();
        
        // Простая схема: первые 4 байта - количество строк
        int rowCount = buffer.getInt();
        
        if (slot >= rowCount) {
            return null;
        }
        
        // Пропускаем заголовок (4 байта) и читаем строку
        int offset = 4 + slot * estimateRowSize();
        if (offset >= Page.PAGE_SIZE) {
            return null;
        }
        
        buffer.position(offset);
        Row row = new Row();
        
        for (TableMetadata.Column col : metadata.getColumns()) {
            Object value = readValue(buffer, col.getType());
            row.addValue(value);
        }
        
        return row;
    }
    
    private Object readValue(ByteBuffer buffer, String type) {
        switch (type.toUpperCase()) {
            case "INT":
            case "INTEGER":
                return buffer.getInt();
            case "VARCHAR":
            case "STRING":
                int len = buffer.getInt();
                byte[] bytes = new byte[len];
                buffer.get(bytes);
                return new String(bytes);
            default:
                return null;
        }
    }
    
    private int estimateRowSize() {
        int size = 0;
        for (TableMetadata.Column col : metadata.getColumns()) {
            switch (col.getType().toUpperCase()) {
                case "INT":
                case "INTEGER":
                    size += 4;
                    break;
                case "VARCHAR":
                case "STRING":
                    size += 4 + 100; // Примерная длина
                    break;
            }
        }
        return size;
    }
}

