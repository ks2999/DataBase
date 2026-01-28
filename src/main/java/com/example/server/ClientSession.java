package com.example.server;

import java.io.*;
import java.net.Socket;

/**
 * Сессия клиента - обработка одного соединения с формализованным протоколом
 */
public class ClientSession implements Runnable {
    private Socket socket;
    private DatabaseServer server;
    private BufferedReader in;
    private PrintWriter out;
    
    public ClientSession(Socket socket, DatabaseServer server) {
        this.socket = socket;
        this.server = server;
    }
    
    public ClientSession(Socket socket, DatabaseServer server, PrintWriter out) {
        this.socket = socket;
        this.server = server;
        this.out = out; // Используем уже созданный PrintWriter
    }
    
    @Override
    public void run() {
        try {
            System.err.println("[SESSION] Client connected: " + socket.getRemoteSocketAddress());
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            
            // Если PrintWriter не был передан, создаем новый
            if (out == null) {
                out = new PrintWriter(socket.getOutputStream(), true);
            }
            
            // Приветствие уже отправлено в DatabaseServer.start()
            // Просто читаем запросы
            
            String line;
            while ((line = in.readLine()) != null) {
                System.err.println("[SESSION] Received: " + line);
                if (line.trim().equalsIgnoreCase("END") || line.trim().equalsIgnoreCase("EXIT")) {
                    break;
                }
                
                if (line.trim().isEmpty()) {
                    continue;
                }
                
                // Разделяем запросы по точке с запятой
                String[] queries = line.split(";");
                for (String query : queries) {
                    query = query.trim();
                    if (query.isEmpty()) {
                        continue;
                    }
                    
                    // Удаляем SQL комментарии (-- до конца строки)
                    int commentPos = query.indexOf("--");
                    if (commentPos >= 0) {
                        query = query.substring(0, commentPos).trim();
                    }
                    
                    // Удаляем пробелы в начале и конце
                    query = query.trim();
                    
                    if (query.isEmpty()) {
                        continue;
                    }
                    
                    // Выполняем запрос
                    System.err.println("[SESSION] Executing query: [" + query + "]");
                    DatabaseServer.QueryResult result = server.executeQuery(query);
                    System.err.println("[SESSION] Query result: " + result.getMessage());
                    
                    // Отправляем результат в простом формате
                    if (result.isSuccess()) {
                        out.println("OK: " + result.getMessage());
                        
                        if (result.getRows() != null && !result.getRows().isEmpty()) {
                            // Заголовки
                            if (result.getColumns() != null && !result.getColumns().isEmpty()) {
                                out.println(String.join(" | ", result.getColumns()));
                                out.println("---");
                            }
                            
                            // Данные
                            for (com.example.executor.Row row : result.getRows()) {
                                StringBuilder sb = new StringBuilder();
                                for (int i = 0; i < row.size(); i++) {
                                    if (i > 0) sb.append(" | ");
                                    Object val = row.getValue(i);
                                    sb.append(val != null ? val.toString() : "NULL");
                                }
                                out.println(sb.toString());
                            }
                        }
                    } else {
                        out.println("ERROR: " + result.getMessage());
                    }
                    
                    out.println("---END---");
                    out.flush();
                    
                    // Убеждаемся, что все отправлено
                    try {
                        socket.getOutputStream().flush();
                        // Даем время на отправку всех данных
                        Thread.sleep(100);
                    } catch (Exception e) {
                        // Игнорируем
                    }
                }
            }
            
        } catch (IOException e) {
            System.err.println("Client session error: " + e.getMessage());
        } catch (Exception e) {
            try {
                out.println("ERROR: Internal error: " + e.getMessage());
                out.println("---END---");
                out.flush();
            } catch (Exception ex) {
                // Игнорируем
            }
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                // Игнорируем
            }
        }
    }
}

