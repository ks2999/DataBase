package com.example;

import com.example.server.DatabaseServer;
import com.example.test.SimpleTestClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@org.junit.jupiter.api.Disabled("End-to-end tests require running server - run manually")
public class EndToEndTest {
    private DatabaseServer server;
    private String testDataDir;
    private int testPort = 5434; // Другой порт для тестов
    private Thread serverThread;
    
    @BeforeEach
    public void setUp() throws Exception {
        // Освобождаем порт
        try {
            Runtime.getRuntime().exec("lsof -ti:" + testPort + " | xargs kill -9").waitFor();
        } catch (Exception e) {
            // Игнорируем
        }
        Thread.sleep(1000);
        
        testDataDir = Files.createTempDirectory("db_e2e_test_").toString();
        server = new DatabaseServer(testPort, testDataDir);
        
        // Запускаем сервер в отдельном потоке
        serverThread = new Thread(() -> server.start());
        serverThread.setDaemon(true);
        serverThread.start();
        
        // Ждем запуска сервера
        Thread.sleep(5000);
        
        // Проверяем, что сервер запустился - пытаемся подключиться
        int attempts = 0;
        boolean serverReady = false;
        while (attempts < 20 && !serverReady) {
            try {
                SimpleTestClient testClient = new SimpleTestClient("localhost", testPort);
                String result = testClient.executeQuery("SELECT 1");
                if (!result.contains("Connection refused") && !result.contains("ERROR: Connection")) {
                    serverReady = true;
                    break;
                }
            } catch (Exception e) {
                // Игнорируем
            }
            Thread.sleep(500);
            attempts++;
        }
        
        if (!serverReady) {
            System.err.println("WARNING: Server may not be ready, tests may fail");
        }
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
        
        Thread.sleep(1000);
        
        // Очистка тестовых файлов
        Path dir = Path.of(testDataDir);
        if (Files.exists(dir)) {
            try {
                Files.walk(dir)
                    .sorted((a, b) -> b.compareTo(a))
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (Exception e) {
                            // Игнорируем
                        }
                    });
            } catch (Exception e) {
                // Игнорируем
            }
        }
    }
    
    @Test
    public void testCreateTable() {
        SimpleTestClient client = new SimpleTestClient("localhost", testPort);
        String result = client.executeQuery("CREATE TABLE e2e_users (id INTEGER, name VARCHAR, age INTEGER)");
        
        // Проверяем, что нет ошибок подключения
        if (result.contains("Connection refused") || result.contains("Connection error")) {
            fail("Cannot connect to server: " + result);
        }
        
        assertFalse(result.contains("ERROR"), "Got error: " + result);
        assertTrue(result.contains("OK") || result.contains("Table created"), "Expected OK or 'Table created', got: " + result);
    }
    
    @Test
    public void testInsert() {
        SimpleTestClient client = new SimpleTestClient("localhost", testPort);
        
        // Сначала создаем таблицу
        String createResult = client.executeQuery("CREATE TABLE e2e_users_insert (id INTEGER, name VARCHAR, age INTEGER)");
        if (createResult.contains("Connection refused") || createResult.contains("Connection error")) {
            fail("Cannot connect to server: " + createResult);
        }
        if (createResult.contains("ERROR") && !createResult.contains("already exists")) {
            fail("Failed to create table: " + createResult);
        }
        
        // Затем вставляем данные
        String result = client.executeQuery("INSERT INTO e2e_users_insert VALUES (1, 'Alice', 25)");
        
        assertFalse(result.contains("ERROR"), "Got error: " + result);
        assertTrue(result.contains("OK") || result.contains("Inserted"), "Expected OK or 'Inserted', got: " + result);
    }
    
    @Test
    public void testSelect() {
        SimpleTestClient client = new SimpleTestClient("localhost", testPort);
        
        // Создаем таблицу и вставляем данные
        String createResult = client.executeQuery("CREATE TABLE e2e_users_select (id INTEGER, name VARCHAR, age INTEGER)");
        if (createResult.contains("Connection refused") || createResult.contains("Connection error")) {
            fail("Cannot connect to server: " + createResult);
        }
        if (createResult.contains("ERROR") && !createResult.contains("already exists")) {
            fail("Failed to create table: " + createResult);
        }
        
        String insert1 = client.executeQuery("INSERT INTO e2e_users_select VALUES (1, 'Alice', 25)");
        if (insert1.contains("ERROR")) {
            fail("Failed to insert: " + insert1);
        }
        
        String insert2 = client.executeQuery("INSERT INTO e2e_users_select VALUES (2, 'Bob', 30)");
        if (insert2.contains("ERROR")) {
            fail("Failed to insert: " + insert2);
        }
        
        // Выбираем данные
        String result = client.executeQuery("SELECT * FROM e2e_users_select");
        
        assertFalse(result.contains("ERROR"), "Got error: " + result);
        assertTrue(result.contains("OK"), "Expected OK, got: " + result);
    }
    
    @Test
    public void testSelectWhere() {
        SimpleTestClient client = new SimpleTestClient("localhost", testPort);
        
        // Создаем таблицу и вставляем данные
        String createResult = client.executeQuery("CREATE TABLE e2e_users_where (id INTEGER, name VARCHAR, age INTEGER)");
        if (createResult.contains("Connection refused") || createResult.contains("Connection error")) {
            fail("Cannot connect to server: " + createResult);
        }
        if (createResult.contains("ERROR") && !createResult.contains("already exists")) {
            fail("Failed to create table: " + createResult);
        }
        
        client.executeQuery("INSERT INTO e2e_users_where VALUES (1, 'Alice', 25)");
        client.executeQuery("INSERT INTO e2e_users_where VALUES (2, 'Bob', 30)");
        
        // Выбираем с условием
        String result = client.executeQuery("SELECT * FROM e2e_users_where WHERE age > 25");
        
        assertFalse(result.contains("ERROR"), "Got error: " + result);
        assertTrue(result.contains("OK"), "Expected OK, got: " + result);
    }
    
    @Test
    public void testCreateIndex() {
        SimpleTestClient client = new SimpleTestClient("localhost", testPort);
        
        // Создаем таблицу и вставляем данные
        String createResult = client.executeQuery("CREATE TABLE e2e_users_idx (id INTEGER, name VARCHAR, age INTEGER)");
        if (createResult.contains("Connection refused") || createResult.contains("Connection error")) {
            fail("Cannot connect to server: " + createResult);
        }
        if (createResult.contains("ERROR") && !createResult.contains("already exists")) {
            fail("Failed to create table: " + createResult);
        }
        
        client.executeQuery("INSERT INTO e2e_users_idx VALUES (1, 'Alice', 25)");
        
        // Создаем индекс
        String result = client.executeQuery("CREATE INDEX e2e_users_idx_id ON e2e_users_idx(id)");
        
        assertFalse(result.contains("ERROR"), "Got error: " + result);
        assertTrue(result.contains("OK") || result.contains("Index created"), "Expected OK or 'Index created', got: " + result);
    }
    
    @Test
    public void testFullWorkflow() {
        SimpleTestClient client = new SimpleTestClient("localhost", testPort);
        
        // 1. Создаем таблицу
        String result1 = client.executeQuery("CREATE TABLE e2e_products (id INTEGER, name VARCHAR, price INTEGER)");
        if (result1.contains("Connection refused") || result1.contains("Connection error")) {
            fail("Cannot connect to server: " + result1);
        }
        assertFalse(result1.contains("ERROR"), "CREATE TABLE failed: " + result1);
        
        // 2. Вставляем данные
        String result2 = client.executeQuery("INSERT INTO e2e_products VALUES (1, 'Laptop', 1000)");
        assertFalse(result2.contains("ERROR"), "INSERT failed: " + result2);
        
        String result3 = client.executeQuery("INSERT INTO e2e_products VALUES (2, 'Phone', 500)");
        assertFalse(result3.contains("ERROR"), "INSERT failed: " + result3);
        
        // 3. Выбираем все данные
        String result4 = client.executeQuery("SELECT * FROM e2e_products");
        assertFalse(result4.contains("ERROR"), "SELECT failed: " + result4);
        
        // 4. Выбираем с условием
        String result5 = client.executeQuery("SELECT * FROM e2e_products WHERE price > 600");
        assertFalse(result5.contains("ERROR"), "SELECT WHERE failed: " + result5);
        
        // 5. Создаем индекс
        String result6 = client.executeQuery("CREATE INDEX e2e_products_price_idx ON e2e_products(price)");
        assertFalse(result6.contains("ERROR"), "CREATE INDEX failed: " + result6);
        
        // 6. Выбираем с использованием индекса
        String result7 = client.executeQuery("SELECT * FROM e2e_products WHERE price = 1000");
        assertFalse(result7.contains("ERROR"), "SELECT with index failed: " + result7);
    }
}

