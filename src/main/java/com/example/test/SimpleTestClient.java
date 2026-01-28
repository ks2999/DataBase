package com.example.test;

import java.io.*;
import java.net.Socket;

/**
 * Простой тестовый клиент для автоматических тестов
 */
public class SimpleTestClient {
    private String host;
    private int port;
    
    public SimpleTestClient(String host, int port) {
        this.host = host;
        this.port = port;
    }
    
    public String executeQuery(String sql) {
        Socket socket = null;
        try {
            socket = new Socket(host, port);
            socket.setSoTimeout(10000); // 10 секунд таймаут
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            
            // Читаем приветствие (сервер отправляет сразу)
            String line1 = in.readLine();
            if (line1 == null || !line1.startsWith("PROTOCOL")) {
                if (socket != null) socket.close();
                return "ERROR: No protocol greeting";
            }
            
            String line2 = in.readLine();
            if (line2 == null || !line2.equals("READY")) {
                if (socket != null) socket.close();
                return "ERROR: No READY message";
            }
            
            // Отправляем запрос
            out.println(sql);
            out.flush();
            
            // Небольшая задержка для обработки
            Thread.sleep(100);
            
            // Небольшая задержка для обработки
            Thread.sleep(100);
            
            // Читаем ответ
            StringBuilder response = new StringBuilder();
            String line;
            int linesRead = 0;
            int maxLines = 100;
            boolean foundEnd = false;
            
            while (linesRead < maxLines) {
                line = in.readLine();
                if (line == null) {
                    // Если уже прочитали что-то, возвращаем
                    if (response.length() > 0) {
                        break;
                    }
                    // Иначе ждем еще
                    Thread.sleep(100);
                    continue;
                }
                
                linesRead++;
                if (line.equals("---END---")) {
                    foundEnd = true;
                    break;
                }
                if (response.length() > 0) {
                    response.append("\n");
                }
                response.append(line);
            }
            
            if (socket != null) socket.close();
            
            if (!foundEnd && response.length() == 0) {
                return "ERROR: No response received (timeout)";
            }
            
            if (response.length() == 0) {
                return "ERROR: Empty response";
            }
            
            return response.toString();
            
        } catch (java.net.SocketTimeoutException e) {
            if (socket != null) try { socket.close(); } catch (Exception ex) {}
            return "ERROR: Read timed out";
        } catch (IOException e) {
            if (socket != null) try { socket.close(); } catch (Exception ex) {}
            return "ERROR: " + e.getMessage();
        } catch (InterruptedException e) {
            if (socket != null) try { socket.close(); } catch (Exception ex) {}
            return "ERROR: Interrupted";
        } catch (Exception e) {
            if (socket != null) try { socket.close(); } catch (Exception ex) {}
            return "ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage();
        }
    }
    
    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 5433;
        
        SimpleTestClient client = new SimpleTestClient(host, port);
        
        if (args.length > 2) {
            // Выполняем один запрос
            String sql = args[2];
            String result = client.executeQuery(sql);
            System.out.println(result);
        } else {
            // Тестовые запросы
            System.out.println("Test 1: CREATE TABLE");
            String result1 = client.executeQuery("CREATE TABLE test_users (id INTEGER, name VARCHAR, age INTEGER)");
            System.out.println(result1);
            System.out.println(result1.contains("OK") ? "PASS" : "FAIL");
            
            System.out.println("\nTest 2: INSERT");
            String result2 = client.executeQuery("INSERT INTO test_users VALUES (1, 'Alice', 25)");
            System.out.println(result2);
            System.out.println(result2.contains("OK") ? "PASS" : "FAIL");
            
            System.out.println("\nTest 3: SELECT");
            String result3 = client.executeQuery("SELECT * FROM test_users");
            System.out.println(result3);
            System.out.println(result3.contains("Alice") ? "PASS" : "FAIL");
            
            System.out.println("\nTest 4: SELECT WHERE");
            String result4 = client.executeQuery("SELECT * FROM test_users WHERE age = 25");
            System.out.println(result4);
            System.out.println(result4.contains("Alice") ? "PASS" : "FAIL");
            
            System.out.println("\nTest 5: CREATE INDEX");
            String result5 = client.executeQuery("CREATE INDEX test_idx ON test_users(id)");
            System.out.println(result5);
            System.out.println(result5.contains("OK") ? "PASS" : "FAIL");
        }
    }
}

