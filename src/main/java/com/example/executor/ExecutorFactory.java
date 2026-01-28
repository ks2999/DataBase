package com.example.executor;

import com.example.buffer.BufferManager;
import com.example.index.IndexManager;
import com.example.sql.optimizer.PhysicalPlan;
import com.example.storage.StorageManager;
import com.example.storage.TableMetadata;

/**
 * Фабрика executors - создание дерева executors из физического плана
 */
public class ExecutorFactory {
    private StorageManager storageManager;
    private BufferManager bufferManager;
    private IndexManager indexManager;
    
    public ExecutorFactory(StorageManager storageManager,
                          BufferManager bufferManager,
                          IndexManager indexManager) {
        this.storageManager = storageManager;
        this.bufferManager = bufferManager;
        this.indexManager = indexManager;
    }
    
    public Executor createExecutor(PhysicalPlan.PhysicalOperator operator,
                                   TableMetadata metadata) {
        if (operator instanceof PhysicalPlan.SeqScanOperator) {
            PhysicalPlan.SeqScanOperator scan = 
                (PhysicalPlan.SeqScanOperator) operator;
            return new SeqScanExecutor(storageManager, bufferManager, 
                                     scan.getTableName());
            
        } else if (operator instanceof PhysicalPlan.IndexScanOperator) {
            PhysicalPlan.IndexScanOperator indexScan = 
                (PhysicalPlan.IndexScanOperator) operator;
            IndexScanExecutor executor = new IndexScanExecutor(storageManager, bufferManager,
                                       indexScan.getTableName(),
                                       indexScan.getIndexName(),
                                       indexScan.getColumnName(),
                                       indexScan.getValue());
            // IndexScanExecutor использует indexManager внутри, что нормально
            return executor;
            
        } else if (operator instanceof PhysicalPlan.FilterOperator) {
            PhysicalPlan.FilterOperator filter = 
                (PhysicalPlan.FilterOperator) operator;
            Executor child = createExecutor(filter.getChildren().get(0), metadata);
            int columnIndex = metadata.getColumnIndex(filter.getColumnName());
            if (columnIndex == -1) {
                throw new RuntimeException("Column not found: " + filter.getColumnName());
            }
            return new FilterExecutor(child, filter.getColumnName(),
                                    filter.getOperator(), filter.getValue(),
                                    columnIndex);
            
        } else if (operator instanceof PhysicalPlan.ProjectOperator) {
            PhysicalPlan.ProjectOperator project = 
                (PhysicalPlan.ProjectOperator) operator;
            Executor child = createExecutor(project.getChildren().get(0), metadata);
            return new ProjectExecutor(child, project.getColumns(), metadata);
        }
        
        throw new RuntimeException("Unknown operator type: " + operator.getClass());
    }
}

