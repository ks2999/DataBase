package com.example.sql.optimizer;

import com.example.index.IndexManager;
import com.example.sql.planner.LogicalPlan;
import com.example.storage.StorageManager;

/**
 * Оптимизатор - преобразование логического плана в физический
 */
public class Optimizer {
    private StorageManager storageManager;
    private IndexManager indexManager;
    
    public Optimizer(StorageManager storageManager, IndexManager indexManager) {
        this.storageManager = storageManager;
        this.indexManager = indexManager;
    }
    
    public PhysicalPlan optimize(LogicalPlan logicalPlan) {
        PhysicalPlan physicalPlan = new PhysicalPlan(
            PhysicalPlan.Type.valueOf(logicalPlan.getType().name()));
        
        physicalPlan.setTableName(logicalPlan.getTableName());
        
        switch (logicalPlan.getType()) {
            case CREATE_TABLE:
                for (LogicalPlan.ColumnDef col : logicalPlan.getColumns()) {
                    physicalPlan.getColumns().add(
                        new PhysicalPlan.ColumnDef(col.getName(), col.getType()));
                }
                break;
                
            case CREATE_INDEX:
                for (LogicalPlan.ColumnDef col : logicalPlan.getColumns()) {
                    physicalPlan.getColumns().add(
                        new PhysicalPlan.ColumnDef(col.getName(), col.getType()));
                }
                break;
                
            case DROP_TABLE:
                // Для DROP TABLE не нужно дополнительных действий
                break;
                
            case INSERT:
                physicalPlan.getInsertValues().addAll(logicalPlan.getInsertValues());
                break;
                
            case SELECT:
                physicalPlan.getSelectColumns().addAll(logicalPlan.getSelectColumns());
                
                // Преобразуем логические операторы в физические
                PhysicalPlan.PhysicalOperator root = 
                    optimizeOperator(logicalPlan.getRootOperator());
                physicalPlan.setRootOperator(root);
                break;
        }
        
        return physicalPlan;
    }
    
    private PhysicalPlan.PhysicalOperator optimizeOperator(
            LogicalPlan.LogicalOperator logicalOp) {
        
        if (logicalOp instanceof LogicalPlan.ScanOperator) {
            LogicalPlan.ScanOperator scan = (LogicalPlan.ScanOperator) logicalOp;
            String tableName = scan.getTableName();
            
            // Пока используем SeqScan, оптимизация индексов будет в следующем слое
            return new PhysicalPlan.SeqScanOperator(tableName);
            
        } else if (logicalOp instanceof LogicalPlan.FilterOperator) {
            LogicalPlan.FilterOperator filter = (LogicalPlan.FilterOperator) logicalOp;
            
            // Проверяем, можно ли использовать индекс
            PhysicalPlan.PhysicalOperator child = 
                optimizeOperator(filter.getChildren().get(0));
            
            // Если дочерний оператор - Scan, проверяем возможность IndexScan
            if (child instanceof PhysicalPlan.SeqScanOperator) {
                PhysicalPlan.SeqScanOperator seqScan = 
                    (PhysicalPlan.SeqScanOperator) child;
                String tableName = seqScan.getTableName();
                
                // Ищем индекс для колонки в условии фильтра
                com.example.index.BPlusTree index = 
                    indexManager.findIndexForColumn(tableName, filter.getColumnName());
                
                if (index != null) {
                    // Используем IndexScan вместо SeqScan
                    String indexName = findIndexNameForColumn(tableName, filter.getColumnName());
                    if (indexName == null) {
                        indexName = tableName + "_" + filter.getColumnName() + "_idx";
                    }
                    PhysicalPlan.IndexScanOperator indexScan = new PhysicalPlan.IndexScanOperator(
                        tableName, indexName, filter.getColumnName(), filter.getValue());
                    // IndexScan заменяет SeqScan, поэтому не добавляем Filter поверх
                    return indexScan;
                }
            }
            
            // Обычный Filter
            PhysicalPlan.FilterOperator physicalFilter = 
                new PhysicalPlan.FilterOperator(
                    filter.getColumnName(), 
                    filter.getOperator(), 
                    filter.getValue());
            physicalFilter.addChild(child);
            return physicalFilter;
            
        } else if (logicalOp instanceof LogicalPlan.ProjectOperator) {
            LogicalPlan.ProjectOperator project = (LogicalPlan.ProjectOperator) logicalOp;
            PhysicalPlan.PhysicalOperator child = 
                optimizeOperator(project.getChildren().get(0));
            
            PhysicalPlan.ProjectOperator physicalProject = 
                new PhysicalPlan.ProjectOperator(project.getColumns());
            physicalProject.addChild(child);
            return physicalProject;
        }
        
        throw new RuntimeException("Unknown logical operator: " + logicalOp.getClass());
    }
    
    private String findIndexName(String tableName, String columnName) {
        // Ищем реальное имя индекса
        java.nio.file.Path dir = java.nio.file.Paths.get("data");
        if (java.nio.file.Files.exists(dir)) {
            try {
                java.nio.file.Files.list(dir).forEach(path -> {
                    String fileName = path.getFileName().toString();
                    if (fileName.endsWith(".idxmeta")) {
                        try (java.io.ObjectInputStream ois = new java.io.ObjectInputStream(
                                new java.io.FileInputStream(path.toFile()))) {
                            com.example.index.IndexManager.IndexMetadata meta = 
                                (com.example.index.IndexManager.IndexMetadata) ois.readObject();
                            if (meta.tableName.equals(tableName) && 
                                meta.columnName.equalsIgnoreCase(columnName)) {
                                // Найден индекс
                            }
                        } catch (Exception e) {
                            // Игнорируем
                        }
                    }
                });
            } catch (java.io.IOException e) {
                // Игнорируем
            }
        }
        
        // Возвращаем стандартное имя индекса
        return tableName + "_" + columnName + "_idx";
    }
    
    public String findIndexNameForColumn(String tableName, String columnName) {
        java.nio.file.Path dir = java.nio.file.Paths.get("data");
        if (java.nio.file.Files.exists(dir)) {
            try {
                final String[] foundIndex = new String[1];
                java.nio.file.Files.list(dir).forEach(path -> {
                    String fileName = path.getFileName().toString();
                    if (fileName.endsWith(".idxmeta")) {
                        String indexName = fileName.substring(0, fileName.length() - 8);
                        try (java.io.ObjectInputStream ois = new java.io.ObjectInputStream(
                                new java.io.FileInputStream(path.toFile()))) {
                            com.example.index.IndexManager.IndexMetadata meta = 
                                (com.example.index.IndexManager.IndexMetadata) ois.readObject();
                            if (meta.tableName.equals(tableName) && 
                                meta.columnName.equalsIgnoreCase(columnName)) {
                                foundIndex[0] = indexName;
                            }
                        } catch (Exception e) {
                            // Игнорируем
                        }
                    }
                });
                if (foundIndex[0] != null) {
                    return foundIndex[0];
                }
            } catch (java.io.IOException e) {
                // Игнорируем
            }
        }
        return null;
    }
}

