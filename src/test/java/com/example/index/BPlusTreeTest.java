package com.example.index;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class BPlusTreeTest {
    private String testDataDir;
    
    @org.junit.jupiter.api.BeforeEach
    public void setUp() throws Exception {
        testDataDir = Files.createTempDirectory("db_index_test_").toString();
    }
    
    @org.junit.jupiter.api.AfterEach
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
    public void testInsertAndSearch() {
        BPlusTree tree = new BPlusTree("test_idx", "test_table", "id", testDataDir);
        
        tree.insert(Integer.valueOf(1), 0, 0);
        tree.insert(Integer.valueOf(2), 0, 1);
        tree.insert(Integer.valueOf(3), 0, 2);
        
        List<BPlusTree.IndexEntry> results = tree.search(Integer.valueOf(2));
        assertFalse(results.isEmpty());
        assertEquals(Integer.valueOf(2), results.get(0).getKey());
    }
    
    @Test
    public void testRangeScan() {
        BPlusTree tree = new BPlusTree("test_idx", "test_table", "id", testDataDir);
        
        for (int i = 1; i <= 10; i++) {
            tree.insert(Integer.valueOf(i), 0, i - 1);
        }
        
        List<BPlusTree.IndexEntry> results = tree.rangeScan(Integer.valueOf(3), Integer.valueOf(7));
        assertEquals(5, results.size());
        assertEquals(Integer.valueOf(3), results.get(0).getKey());
        assertEquals(Integer.valueOf(7), results.get(4).getKey());
    }
    
    @Test
    public void testIsEmpty() {
        BPlusTree tree = new BPlusTree("test_idx", "test_table", "id", testDataDir);
        assertTrue(tree.isEmpty());
        
        tree.insert(Integer.valueOf(1), 0, 0);
        assertFalse(tree.isEmpty());
    }
}

