package com.example.test;

import java.io.*;
import java.net.Socket;

/**
 * Надежный тестовый клиент для автоматических тестов
 */
public class AutoTestClient {
    
    public static String executeQuery(String host, int port, String sql) {
        Socket socket = null;
        try {
            // Подключаемся
            socket = new Socket(host, port);
            socket.setSoTimeout(25000); // 25 секунд
            
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            
            // Ждем и читаем приветствие
            String line1 = waitForLine(in, "PROTOCOL", 5000);
            if (line1 == null) {
                socket.close();
                return "ERROR: No PROTOCOL greeting";
            }
            
            String line2 = waitForLine(in, "READY", 5000);
            if (line2 == null) {
                socket.close();
                return "ERROR: No READY message";
            }
            
            // Отправляем запрос
            out.println(sql);
            out.flush();
            
            // Ждем обработки
            Thread.sleep(3000);
            
            // Читаем ответ
            StringBuilder response = new StringBuilder();
            String line;
            long startTime = System.currentTimeMillis();
            
            while (System.currentTimeMillis() - startTime < 15000) {
                try {
                    line = in.readLine();
                    if (line == null) {
                        if (response.length() > 0) {
                            break; // Уже есть ответ
                        }
                        Thread.sleep(200);
                        continue;
                    }
                    
                    if (line.equals("---END---")) {
                        break;
                    }
                    
                    if (response.length() > 0) {
                        response.append("\n");
                    }
                    response.append(line);
                } catch (java.net.SocketTimeoutException e) {
                    if (response.length() > 0) {
                        break;
                    }
                    Thread.sleep(200);
                }
            }
            
            socket.close();
            return response.length() > 0 ? response.toString() : "ERROR: No response";
            
        } catch (Exception e) {
            if (socket != null) {
                try { socket.close(); } catch (Exception ex) {}
            }
            return "ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage();
        }
    }
    
    private static String waitForLine(BufferedReader in, String prefix, long timeoutMs) {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < timeoutMs) {
            try {
                String line = in.readLine();
                if (line != null && line.startsWith(prefix)) {
                    return line;
                }
            } catch (java.net.SocketTimeoutException e) {
                // Продолжаем ждать
            } catch (IOException e) {
                return null;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                return null;
            }
        }
        return null;
    }
    
    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 5432;
        String sql = args.length > 2 ? args[2] : "";
        
        if (sql.isEmpty()) {
            System.out.println("Usage: AutoTestClient <host> <port> <sql>");
            return;
        }
        
        String result = executeQuery(host, port, sql);
        System.out.println(result);
    }
}
