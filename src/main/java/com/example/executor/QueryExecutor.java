package com.example.executor;

import com.example.buffer.BufferManager;
import com.example.index.BPlusTree;
import com.example.index.IndexManager;
import com.example.sql.optimizer.PhysicalPlan;
import com.example.storage.Page;
import com.example.storage.StorageManager;
import com.example.storage.TableFile;
import com.example.storage.TableMetadata;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Главный исполнитель запросов
 */
public class QueryExecutor {
    private StorageManager storageManager;
    private BufferManager bufferManager;
    private IndexManager indexManager;
    private ExecutorFactory executorFactory;
    
    public QueryExecutor(StorageManager storageManager,
                        BufferManager bufferManager,
                        IndexManager indexManager) {
        this.storageManager = storageManager;
        this.bufferManager = bufferManager;
        this.indexManager = indexManager;
        this.executorFactory = new ExecutorFactory(storageManager, 
                                                  bufferManager, indexManager);
    }
    
    public QueryResult execute(PhysicalPlan plan) {
        switch (plan.getType()) {
            case CREATE_TABLE:
                return executeCreateTable(plan);
            case CREATE_INDEX:
                return executeCreateIndex(plan);
            case DROP_TABLE:
                return executeDropTable(plan);
            case INSERT:
                return executeInsert(plan);
            case SELECT:
                return executeSelect(plan);
            default:
                throw new RuntimeException("Unknown plan type: " + plan.getType());
        }
    }
    
    private QueryResult executeDropTable(PhysicalPlan plan) {
        String tableName = plan.getTableName();
        
        if (!storageManager.tableExists(tableName)) {
            throw new RuntimeException("Table does not exist: " + tableName);
        }
        
        // Удаляем все индексы для этой таблицы
        indexManager.dropIndexesForTable(tableName);
        
        // Удаляем таблицу (включая все файлы)
        storageManager.dropTable(tableName);
        
        return new QueryResult(true, "Table dropped: " + tableName);
    }
    
    private QueryResult executeCreateIndex(PhysicalPlan plan) {
        if (plan.getColumns().isEmpty()) {
            throw new RuntimeException("Invalid CREATE INDEX: missing index name or column");
        }
        
        String indexName = plan.getColumns().get(0).getName();
        String columnName = plan.getColumns().get(0).getType();
        String tableName = plan.getTableName();
        
        if (!storageManager.tableExists(tableName)) {
            throw new RuntimeException("Table does not exist: " + tableName);
        }
        
        TableMetadata metadata = storageManager.getTableMetadata(tableName);
        if (metadata.getColumn(columnName) == null) {
            throw new RuntimeException("Column does not exist: " + columnName);
        }
        
        indexManager.createIndex(indexName, tableName, columnName);
        
        // Построить индекс для существующих данных
        buildIndexForExistingData(indexName, tableName, columnName);
        
        return new QueryResult(true, "Index created: " + indexName);
    }
    
    private void buildIndexForExistingData(String indexName, String tableName, String columnName) {
        BPlusTree index = indexManager.getIndex(indexName);
        if (index == null) {
            return;
        }
        
        TableMetadata metadata = storageManager.getTableMetadata(tableName);
        TableFile tableFile = storageManager.getTableFile(tableName);
        List<Integer> pageIds = tableFile.getPageIds();
        int columnIndex = metadata.getColumnIndex(columnName);
        
        for (int pageId : pageIds) {
            Page page = bufferManager.getPage(tableFile, pageId);
            ByteBuffer buffer = page.getBuffer();
            buffer.rewind();
            
            int rowCount = buffer.getInt();
            for (int slot = 0; slot < rowCount; slot++) {
                int offset = 4 + slot * estimateRowSize(metadata);
                buffer.position(offset);
                
                // Пропускаем колонки до нужной
                for (int i = 0; i < columnIndex; i++) {
                    skipValue(buffer, metadata.getColumns().get(i).getType());
                }
                
                // Читаем значение для индекса
                Object key = readValue(buffer, metadata.getColumns().get(columnIndex).getType());
                if (key != null) {
                    @SuppressWarnings("unchecked")
                    Comparable<?> compKey = (Comparable<?>) key;
                    index.insert(compKey, pageId, slot);
                }
            }
        }
        
        index.saveIndex();
    }
    
    private void skipValue(ByteBuffer buffer, String type) {
        switch (type.toUpperCase()) {
            case "INT":
            case "INTEGER":
                buffer.getInt();
                break;
            case "VARCHAR":
            case "STRING":
                int len = buffer.getInt();
                buffer.position(buffer.position() + len);
                break;
        }
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
    
    private QueryResult executeCreateTable(PhysicalPlan plan) {
        TableMetadata metadata = new TableMetadata(plan.getTableName());
        for (PhysicalPlan.ColumnDef col : plan.getColumns()) {
            metadata.addColumn(col.getName(), col.getType());
        }
        storageManager.createTable(metadata);
        return new QueryResult(true, "Table created: " + plan.getTableName());
    }
    
    private QueryResult executeInsert(PhysicalPlan plan) {
        TableMetadata metadata = storageManager.getTableMetadata(plan.getTableName());
        TableFile tableFile = storageManager.getTableFile(plan.getTableName());
        
        // Находим или создаем страницу для вставки
        List<Integer> pageIds = tableFile.getPageIds();
        Page page;
        int pageId;
        int slotId;
        
        if (pageIds.isEmpty()) {
            pageId = tableFile.allocatePage();
            page = new Page(pageId);
            bufferManager.addPage(tableFile, page);
        } else {
            pageId = pageIds.get(pageIds.size() - 1);
            page = bufferManager.getPage(tableFile, pageId);
        }
        
        // Записываем строку в страницу
        ByteBuffer buffer = page.getBuffer();
        buffer.rewind();
        
        // Читаем количество строк
        int rowCount = 0;
        if (buffer.remaining() >= 4) {
            rowCount = buffer.getInt();
        }
        
        // Вычисляем offset для новой строки
        int rowSize = estimateRowSize(metadata);
        int offset = 4 + rowCount * rowSize;
        
        if (offset + rowSize > Page.PAGE_SIZE) {
            // Сохраняем текущую страницу
            bufferManager.markDirty(tableFile, pageId);
            tableFile.savePage(page);
            
            // Нужна новая страница
            pageId = tableFile.allocatePage();
            page = new Page(pageId);
            bufferManager.addPage(tableFile, page);
            buffer = page.getBuffer();
            buffer.rewind();
            buffer.putInt(0);
            rowCount = 0;
            offset = 4;
        }
        
        // Записываем строку
        buffer.position(offset);
        int colIndex = 0;
        for (TableMetadata.Column col : metadata.getColumns()) {
            Object value = plan.getInsertValues().get(colIndex++);
            writeValue(buffer, col.getType(), value);
        }
        
        // Обновляем счетчик строк
        buffer.position(0);
        buffer.putInt(rowCount + 1);
        
        slotId = rowCount;
        
        // Сохраняем страницу
        tableFile.savePage(page);
        bufferManager.markDirty(tableFile, pageId);
        
        // Убеждаемся, что страница в буфере
        bufferManager.pinPage(tableFile, pageId);
        
        // Обновляем индексы
        updateIndexes(plan.getTableName(), pageId, slotId, metadata, plan.getInsertValues());
        
        return new QueryResult(true, "1 row inserted");
    }
    
    private QueryResult executeSelect(PhysicalPlan plan) {
        TableMetadata metadata = storageManager.getTableMetadata(plan.getTableName());
        Executor executor = executorFactory.createExecutor(plan.getRootOperator(), metadata);
        
        executor.open();
        List<Row> rows = new ArrayList<>();
        Row row;
        while ((row = executor.next()) != null) {
            rows.add(row);
        }
        executor.close();
        
        return new QueryResult(true, rows, plan.getSelectColumns());
    }
    
    private void updateIndexes(String tableName, int pageId, int slotId,
                              TableMetadata metadata, List<Object> values) {
        // Обновляем все индексы для этой таблицы
        for (int i = 0; i < metadata.getColumns().size(); i++) {
            TableMetadata.Column col = metadata.getColumns().get(i);
            String indexName = tableName + "_" + col.getName() + "_idx";
            BPlusTree index = indexManager.getIndex(indexName);
            
            if (index != null) {
                Object key = values.get(i);
                @SuppressWarnings("unchecked")
                Comparable<?> compKey = (Comparable<?>) key;
                index.insert(compKey, pageId, slotId);
            }
        }
        indexManager.saveAll();
    }
    
    private int estimateRowSize(TableMetadata metadata) {
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
    
    private void writeValue(ByteBuffer buffer, String type, Object value) {
        switch (type.toUpperCase()) {
            case "INT":
            case "INTEGER":
                buffer.putInt((Integer) value);
                break;
            case "VARCHAR":
            case "STRING":
                String str = value.toString();
                buffer.putInt(str.length());
                buffer.put(str.getBytes());
                break;
        }
    }
    
    public static class QueryResult {
        private boolean success;
        private String message;
        private List<Row> rows;
        private List<String> columns;
        
        public QueryResult(boolean success, String message) {
            this.success = success;
            this.message = message;
            this.rows = new ArrayList<>();
        }
        
        public QueryResult(boolean success, List<Row> rows, List<String> columns) {
            this.success = success;
            this.rows = rows;
            this.columns = columns;
            this.message = rows.size() + " row(s) returned";
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

