package com.example.client;


import java.io.*;
import java.net.Socket;
import java.util.Scanner;

/**
 * CLI клиент для подключения к серверу СУБД
 */
public class DatabaseClient {
    private String host;
    private int port;
    private Socket socket;
    private BufferedReader in;
    private PrintWriter out;
    private Scanner scanner;
    
    public DatabaseClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.scanner = new Scanner(System.in);
    }
    
    public void connect() {
        try {
            socket = new Socket(host, port);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out = new PrintWriter(socket.getOutputStream(), true);
            
            // Читаем приветствие (блокирующее чтение)
            String line = in.readLine();
            if (line != null && line.startsWith("PROTOCOL:")) {
                System.out.println("Connected to server (" + line + ")");
            } else {
                System.err.println("Error: No PROTOCOL greeting, got: " + line);
                socket.close();
                return;
            }
            
            line = in.readLine();
            if (line != null && line.equals("READY")) {
                System.out.println("Server ready. Type SQL queries (exit to quit):");
            } else {
                System.err.println("Error: No READY message, got: " + line);
                socket.close();
                return;
            }
            
            // Читаем и выводим ответы сервера в отдельном потоке
            Thread readerThread = new Thread(() -> {
                try {
                    String responseLine;
                    while ((responseLine = in.readLine()) != null) {
                        if (responseLine.equals("---END---")) {
                            // Конец ответа - выводим пустую строку для разделения
                            System.out.println();
                            System.out.flush();
                            continue;
                        }
                        if (!responseLine.isEmpty()) {
                            System.out.println(responseLine);
                            System.out.flush();
                        }
                    }
                } catch (IOException e) {
                    // Connection closed - это нормально при выходе
                }
            });
            readerThread.setDaemon(true);
            readerThread.start();
            
            // Даем потоку время на запуск
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // Игнорируем
            }
            
            // Читаем команды от пользователя
            String input;
            while (scanner.hasNextLine()) {
                try {
                    input = scanner.nextLine();
                    if (input == null) {
                        break;
                    }
                    if (input.trim().equalsIgnoreCase("exit") || 
                        input.trim().equalsIgnoreCase("quit")) {
                        out.println("END");
                        out.flush();
                        break;
                    }
                    if (!input.trim().isEmpty()) {
                        out.println(input);
                        out.flush();
                    }
                } catch (java.util.NoSuchElementException e) {
                    // Нет больше ввода
                    break;
                }
            }
            
        } catch (IOException e) {
            System.err.println("Connection error: " + e.getMessage());
        } finally {
            disconnect();
        }
    }
    
    public void disconnect() {
        try {
            if (out != null) out.close();
            if (in != null) in.close();
            if (socket != null) socket.close();
            if (scanner != null) scanner.close();
        } catch (IOException e) {
            // Игнорируем
        }
    }
    
    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 5433;
        
        DatabaseClient client = new DatabaseClient(host, port);
        client.connect();
    }
}

