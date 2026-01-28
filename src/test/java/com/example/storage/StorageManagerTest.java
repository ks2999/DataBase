package com.example.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

public class StorageManagerTest {
    private String testDataDir;
    private StorageManager storageManager;
    
    @BeforeEach
    public void setUp() throws Exception {
        testDataDir = Files.createTempDirectory("db_test_").toString();
        storageManager = new StorageManager(testDataDir);
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        // Очистка тестовых файлов
        Path dir = Path.of(testDataDir);
        if (Files.exists(dir)) {
            Files.walk(dir)
                .sorted((a, b) -> b.compareTo(a))
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (Exception e) {
                        // Игнорируем
                    }
                });
        }
    }
    
    @Test
    public void testCreateTable() {
        TableMetadata metadata = new TableMetadata("test_table");
        metadata.addColumn("id", "INTEGER");
        metadata.addColumn("name", "VARCHAR");
        
        storageManager.createTable(metadata);
        
        assertTrue(storageManager.tableExists("test_table"));
        TableMetadata loaded = storageManager.getTableMetadata("test_table");
        assertNotNull(loaded);
        assertEquals(2, loaded.getColumns().size());
    }
    
    @Test
    public void testTableExists() {
        assertFalse(storageManager.tableExists("nonexistent"));
        
        TableMetadata metadata = new TableMetadata("test_table");
        metadata.addColumn("id", "INTEGER");
        storageManager.createTable(metadata);
        
        assertTrue(storageManager.tableExists("test_table"));
    }
    
    @Test
    public void testGetTableFile() {
        TableMetadata metadata = new TableMetadata("test_table");
        metadata.addColumn("id", "INTEGER");
        storageManager.createTable(metadata);
        
        TableFile tableFile = storageManager.getTableFile("test_table");
        assertNotNull(tableFile);
        assertEquals("test_table", tableFile.getTableName());
    }
}


