package com.example;

import com.example.client.DatabaseClient;
import com.example.server.DatabaseServer;

/**
 * Главный класс для запуска сервера или клиента
 */
public class Main {
    public static void main(String[] args) {
        if (args.length > 0 && args[0].equals("client")) {
            // Запуск клиента
            String host = args.length > 1 ? args[1] : "localhost";
            int port = args.length > 2 ? Integer.parseInt(args[2]) : 5433;
            DatabaseClient client = new DatabaseClient(host, port);
            client.connect();
        } else {
            // Запуск сервера
            int port = args.length > 0 ? Integer.parseInt(args[0]) : 5433;
            String dataDir = args.length > 1 ? args[1] : "data";
            
            DatabaseServer server = new DatabaseServer(port, dataDir);
            
            // Добавляем обработчик завершения для корректного закрытия
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting down server...");
                server.stop();
            }));
            
            server.start();
        }
    }
}
