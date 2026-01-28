package com.example.executor;

import com.example.buffer.BufferManager;
import com.example.index.IndexManager;
import com.example.sql.optimizer.PhysicalPlan;
import com.example.storage.StorageManager;
import com.example.storage.TableMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;

public class QueryExecutorTest {
    private StorageManager storageManager;
    private BufferManager bufferManager;
    private IndexManager indexManager;
    private QueryExecutor queryExecutor;
    private String testDataDir;
    
    @BeforeEach
    public void setUp() throws Exception {
        testDataDir = Files.createTempDirectory("db_executor_test_").toString();
        storageManager = new StorageManager(testDataDir);
        bufferManager = new BufferManager(10);
        indexManager = new IndexManager(testDataDir);
        queryExecutor = new QueryExecutor(storageManager, bufferManager, indexManager);
    }
    
    @Test
    public void testExecuteCreateTable() {
        PhysicalPlan plan = new PhysicalPlan(PhysicalPlan.Type.CREATE_TABLE);
        plan.setTableName("test_table");
        plan.getColumns().add(new PhysicalPlan.ColumnDef("id", "INTEGER"));
        plan.getColumns().add(new PhysicalPlan.ColumnDef("name", "VARCHAR"));
        
        QueryExecutor.QueryResult result = queryExecutor.execute(plan);
        
        assertTrue(result.isSuccess());
        assertTrue(storageManager.tableExists("test_table"));
    }
    
    @Test
    public void testExecuteInsert() {
        // Сначала создаем таблицу
        TableMetadata metadata = new TableMetadata("test_table");
        metadata.addColumn("id", "INTEGER");
        metadata.addColumn("name", "VARCHAR");
        storageManager.createTable(metadata);
        
        PhysicalPlan plan = new PhysicalPlan(PhysicalPlan.Type.INSERT);
        plan.setTableName("test_table");
        plan.getInsertValues().add(1);
        plan.getInsertValues().add("Alice");
        
        QueryExecutor.QueryResult result = queryExecutor.execute(plan);
        
        assertTrue(result.isSuccess());
    }
    
    @Test
    public void testExecuteSelect() {
        // Сначала создаем таблицу и вставляем данные
        TableMetadata metadata = new TableMetadata("test_table");
        metadata.addColumn("id", "INTEGER");
        metadata.addColumn("name", "VARCHAR");
        storageManager.createTable(metadata);
        
        PhysicalPlan insertPlan = new PhysicalPlan(PhysicalPlan.Type.INSERT);
        insertPlan.setTableName("test_table");
        insertPlan.getInsertValues().add(1);
        insertPlan.getInsertValues().add("Alice");
        queryExecutor.execute(insertPlan);
        
        // Теперь выбираем
        PhysicalPlan selectPlan = new PhysicalPlan(PhysicalPlan.Type.SELECT);
        selectPlan.setTableName("test_table");
        selectPlan.getSelectColumns().add("id");
        selectPlan.getSelectColumns().add("name");
        
        PhysicalPlan.SeqScanOperator scan = new PhysicalPlan.SeqScanOperator("test_table");
        selectPlan.setRootOperator(scan);
        
        QueryExecutor.QueryResult result = queryExecutor.execute(selectPlan);
        
        assertTrue(result.isSuccess());
        assertNotNull(result.getRows());
    }
}


