package com.example.executor;

import com.example.buffer.BufferManager;
import com.example.index.BPlusTree;
import com.example.storage.Page;
import com.example.storage.TableFile;
import com.example.storage.TableMetadata;
import com.example.storage.StorageManager;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * IndexScan executor - сканирование через индекс
 */
public class IndexScanExecutor implements Executor {
    private StorageManager storageManager;
    private BufferManager bufferManager;
    private String tableName;
    private String indexName;
    private String columnName;
    private Object value;
    private BPlusTree index;
    private TableMetadata metadata;
    private TableFile tableFile;
    private List<BPlusTree.IndexEntry> indexEntries;
    private int currentIndex;
    private boolean isOpen;
    
    public IndexScanExecutor(StorageManager storageManager,
                            BufferManager bufferManager,
                            String tableName,
                            String indexName,
                            String columnName,
                            Object value) {
        this.storageManager = storageManager;
        this.bufferManager = bufferManager;
        this.tableName = tableName;
        this.indexName = indexName;
        this.columnName = columnName;
        this.value = value;
    }
    
    @Override
    public void open() {
        this.metadata = storageManager.getTableMetadata(tableName);
        this.tableFile = storageManager.getTableFile(tableName);
        
        // Загружаем индекс через IndexManager
        // IndexManager должен быть передан извне, но для упрощения создаем новый
        // В реальной системе это было бы через dependency injection
        com.example.index.IndexManager indexManager = 
            new com.example.index.IndexManager("data");
        this.index = indexManager.getIndex(indexName);
        
        if (this.index == null) {
            // Пытаемся найти индекс по имени колонки
            this.index = indexManager.findIndexForColumn(tableName, columnName);
        }
        
        if (index == null) {
            throw new RuntimeException("Index not found: " + indexName);
        }
        
        // Выполняем поиск по индексу
        @SuppressWarnings("unchecked")
        List<BPlusTree.IndexEntry> entries = index.search((Comparable<?>) value);
        this.indexEntries = entries;
        this.currentIndex = 0;
        this.isOpen = true;
    }
    
    @Override
    public Row next() {
        if (!isOpen || currentIndex >= indexEntries.size()) {
            return null;
        }
        
        BPlusTree.IndexEntry entry = indexEntries.get(currentIndex++);
        
        // Загружаем страницу и читаем строку
        Page page = bufferManager.getPage(tableFile, entry.getPageId());
        return readRowFromPage(page, entry.getSlotId());
    }
    
    @Override
    public void close() {
        this.isOpen = false;
        this.indexEntries = null;
    }
    
    private Row readRowFromPage(Page page, int slot) {
        ByteBuffer buffer = page.getBuffer();
        buffer.rewind();
        
        int rowCount = buffer.getInt();
        if (slot >= rowCount) {
            return null;
        }
        
        int offset = 4 + slot * estimateRowSize();
        buffer.position(offset);
        
        Row row = new Row();
        for (TableMetadata.Column col : metadata.getColumns()) {
            Object val = readValue(buffer, col.getType());
            row.addValue(val);
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
                    size += 4 + 100;
                    break;
            }
        }
        return size;
    }
}

