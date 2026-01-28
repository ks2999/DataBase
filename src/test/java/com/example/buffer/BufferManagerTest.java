package com.example.buffer;

import com.example.storage.Page;
import com.example.storage.TableFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

public class BufferManagerTest {
    private String testDataDir;
    private BufferManager bufferManager;
    
    @BeforeEach
    public void setUp() throws Exception {
        testDataDir = Files.createTempDirectory("db_buffer_test_").toString();
        bufferManager = new BufferManager(10);
    }
    
    @AfterEach
    public void tearDown() throws Exception {
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
    public void testGetPage() {
        TableFile tableFile = new TableFile("test_table", testDataDir);
        
        Page page = bufferManager.getPage(tableFile, 0);
        assertNotNull(page);
        assertEquals(0, page.getPageId());
    }
    
    @Test
    public void testLRU() {
        BufferManager smallBuffer = new BufferManager(3);
        
        TableFile table1 = new TableFile("table1", testDataDir);
        TableFile table2 = new TableFile("table2", testDataDir);
        TableFile table3 = new TableFile("table3", testDataDir);
        TableFile table4 = new TableFile("table4", testDataDir);
        
        // Заполняем буфер
        smallBuffer.getPage(table1, 0);
        smallBuffer.getPage(table2, 0);
        smallBuffer.getPage(table3, 0);
        
        // Добавляем еще одну страницу - должна вытеснить первую
        smallBuffer.getPage(table4, 0);
        
        // Проверяем, что можем получить страницу
        Page page1 = smallBuffer.getPage(table1, 0);
        assertNotNull(page1);
    }
    
    @Test
    public void testFlushAll() {
        TableFile tableFile = new TableFile("test_table", testDataDir);
        
        Page page = bufferManager.getPage(tableFile, 0);
        page.setDirty(true);
        
        bufferManager.flushAll();
        
        // После flush страница не должна быть dirty
        assertFalse(page.isDirty());
    }
}

