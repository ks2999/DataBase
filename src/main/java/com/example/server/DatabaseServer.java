package com.example.server;

import com.example.buffer.BufferManager;
import com.example.executor.QueryExecutor;
import com.example.index.IndexManager;
import com.example.sql.lexer.Lexer;
import com.example.sql.optimizer.Optimizer;
import com.example.sql.optimizer.PhysicalPlan;
import com.example.sql.parser.Parser;
import com.example.sql.planner.LogicalPlan;
import com.example.sql.planner.Planner;
import com.example.sql.semantic.QueryTree;
import com.example.sql.semantic.SemanticAnalyzer;
import com.example.storage.StorageManager;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * TCP сервер СУБД
 */
public class DatabaseServer {
    private int port;
    private StorageManager storageManager;
    private BufferManager bufferManager;
    private IndexManager indexManager;
    private QueryExecutor queryExecutor;
    private Logger logger;
    private boolean running;
    private ExecutorService threadPool;
    
    public DatabaseServer(int port, String dataDir) {
        this.port = port;
        this.storageManager = new StorageManager(dataDir);
        this.bufferManager = new BufferManager(100); // 100 страниц в буфере
        this.indexManager = new IndexManager(dataDir);
        this.queryExecutor = new QueryExecutor(storageManager, bufferManager, indexManager);
        this.logger = new Logger();
        this.threadPool = Executors.newCachedThreadPool();
    }
    
    public void start() {
        running = true;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            logger.log("Server started on port " + port);
            
            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    logger.log("New client connected: " + clientSocket.getRemoteSocketAddress());
                    
                    // Создаем PrintWriter один раз и передаем его в сессию
                    PrintWriter sessionOut = null;
                    try {
                        sessionOut = new PrintWriter(clientSocket.getOutputStream(), true);
                        // Отправляем приветствие СРАЗУ
                        sessionOut.println("PROTOCOL:" + Protocol.VERSION);
                        sessionOut.println("READY");
                        sessionOut.flush();
                        logger.log("Greeting sent to " + clientSocket.getRemoteSocketAddress());
                    } catch (Exception e) {
                        logger.log("Error sending greeting: " + e.getMessage());
                        e.printStackTrace();
                        try {
                            clientSocket.close();
                        } catch (IOException ex) {
                            // Игнорируем
                        }
                        continue;
                    }
                    
                    // Запускаем сессию для обработки запросов с уже созданным PrintWriter
                    ClientSession session = new ClientSession(clientSocket, this, sessionOut);
                    threadPool.submit(session);
                    logger.log("Session submitted to thread pool");
                } catch (Exception e) {
                    logger.log("Error accepting client: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            logger.log("Server error: " + e.getMessage());
        }
    }
    
    public void stop() {
        running = false;
        bufferManager.flushAll();
        indexManager.saveAll();
        threadPool.shutdown();
        logger.log("Server stopped");
    }
    
    public QueryResult executeQuery(String sql) {
        try {
            logger.log("Executing query: " + sql);
            
            // Lexer
            Lexer lexer = new Lexer(sql);
            List<com.example.sql.lexer.Token> tokens = lexer.tokenize();
            logger.log("Tokens: " + tokens);
            // Детальное логирование токенов
            StringBuilder tokenDetails = new StringBuilder("Token details: ");
            for (int i = 0; i < Math.min(tokens.size(), 10); i++) {
                com.example.sql.lexer.Token t = tokens.get(i);
                tokenDetails.append(i).append(":").append(t.getType()).append("('").append(t.getValue()).append("') ");
            }
            logger.log(tokenDetails.toString());
            
            // Parser
            Parser parser = new Parser(tokens);
            com.example.sql.parser.ASTNode ast = parser.parse();
            logger.log("AST type: " + ast.getType() + ", value: '" + ast.getValue() + "'");
            
            // Semantic Analyzer
            SemanticAnalyzer semanticAnalyzer = new SemanticAnalyzer(storageManager);
            QueryTree queryTree = semanticAnalyzer.analyze(ast);
            logger.log("QueryTree: " + queryTree.getType() + " on " + queryTree.getTableName());
            
            // Planner
            Planner planner = new Planner();
            LogicalPlan logicalPlan = planner.plan(queryTree);
            logger.log("LogicalPlan: " + logicalPlan.getType());
            
            // Optimizer
            Optimizer optimizer = new Optimizer(storageManager, indexManager);
            PhysicalPlan physicalPlan = optimizer.optimize(logicalPlan);
            
            // Детальное логирование физического плана
            StringBuilder planLog = new StringBuilder();
            planLog.append("PhysicalPlan: ").append(physicalPlan.getType());
            if (physicalPlan.getRootOperator() != null) {
                planLog.append("\n  Root Operator: ").append(physicalPlan.getRootOperator().getOperatorType());
                logPhysicalPlanTree(physicalPlan.getRootOperator(), planLog, 1);
            }
            logger.log(planLog.toString());
            
            // Executor
            com.example.executor.QueryExecutor.QueryResult result = 
                queryExecutor.execute(physicalPlan);
            
            logger.log("Query result: " + result.getMessage());
            return new QueryResult(true, result.getMessage(), result.getRows(), result.getColumns());
            
        } catch (Exception e) {
            logger.log("Error: " + e.getMessage());
            e.printStackTrace();
            return new QueryResult(false, "Error: " + e.getMessage(), null, null);
        }
    }
    
    public StorageManager getStorageManager() {
        return storageManager;
    }
    
    public IndexManager getIndexManager() {
        return indexManager;
    }
    
    public static class QueryResult {
        private boolean success;
        private String message;
        private List<com.example.executor.Row> rows;
        private List<String> columns;
        
        public QueryResult(boolean success, String message, 
                          List<com.example.executor.Row> rows, 
                          List<String> columns) {
            this.success = success;
            this.message = message;
            this.rows = rows;
            this.columns = columns;
        }
        
        public boolean isSuccess() {
            return success;
        }
        
        public String getMessage() {
            return message;
        }
        
        public List<com.example.executor.Row> getRows() {
            return rows;
        }
        
        public List<String> getColumns() {
            return columns;
        }
    }
    
    private void logPhysicalPlanTree(PhysicalPlan.PhysicalOperator op, StringBuilder sb, int depth) {
        String indent = "  ".repeat(depth);
        
        if (op instanceof PhysicalPlan.SeqScanOperator) {
            PhysicalPlan.SeqScanOperator scan = (PhysicalPlan.SeqScanOperator) op;
            sb.append("\n").append(indent).append("SeqScan(table=").append(scan.getTableName()).append(")");
        } else if (op instanceof PhysicalPlan.IndexScanOperator) {
            PhysicalPlan.IndexScanOperator indexScan = (PhysicalPlan.IndexScanOperator) op;
            sb.append("\n").append(indent).append("IndexScan(table=").append(indexScan.getTableName())
              .append(", index=").append(indexScan.getIndexName())
              .append(", column=").append(indexScan.getColumnName())
              .append(", value=").append(indexScan.getValue()).append(")");
        } else if (op instanceof PhysicalPlan.FilterOperator) {
            PhysicalPlan.FilterOperator filter = (PhysicalPlan.FilterOperator) op;
            sb.append("\n").append(indent).append("Filter(column=").append(filter.getColumnName())
              .append(", op=").append(filter.getOperator())
              .append(", value=").append(filter.getValue()).append(")");
        } else if (op instanceof PhysicalPlan.ProjectOperator) {
            PhysicalPlan.ProjectOperator project = (PhysicalPlan.ProjectOperator) op;
            sb.append("\n").append(indent).append("Project(columns=").append(project.getColumns()).append(")");
        }
        
        for (PhysicalPlan.PhysicalOperator child : op.getChildren()) {
            logPhysicalPlanTree(child, sb, depth + 1);
        }
    }
    
    private static class Logger {
        public void log(String message) {
            System.out.println("[SERVER] " + java.time.LocalDateTime.now() + " - " + message);
        }
    }
}

