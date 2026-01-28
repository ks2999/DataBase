package com.example.sql.semantic;

import com.example.sql.parser.ASTNode;
import com.example.storage.StorageManager;
import com.example.storage.TableMetadata;
import java.util.List;

/**
 * Семантический анализатор - преобразование AST в QueryTree
 */
public class SemanticAnalyzer {
    private StorageManager storageManager;
    
    public SemanticAnalyzer(StorageManager storageManager) {
        this.storageManager = storageManager;
    }
    
    public QueryTree analyze(ASTNode ast) {
        if (ast.getType() == ASTNode.Type.CREATE_TABLE && ast.getValue() != null && ast.getValue().startsWith("INDEX:")) {
            return analyzeCreateIndex(ast);
        }
        
        switch (ast.getType()) {
            case CREATE_TABLE:
                return analyzeCreateTable(ast);
            case DROP_TABLE:
                return analyzeDropTable(ast);
            case INSERT:
                return analyzeInsert(ast);
            case SELECT:
                return analyzeSelect(ast);
            default:
                throw new RuntimeException("Unsupported AST node type: " + ast.getType());
        }
    }
    
    private QueryTree analyzeCreateIndex(ASTNode ast) {
        QueryTree query = new QueryTree(QueryTree.Type.CREATE_INDEX);
        String[] parts = ast.getValue().split(":");
        if (parts.length >= 4) {
            query.setTableName(parts[2]); // tableName
            // Сохраняем indexName и columnName в специальном формате
            query.getColumns().add(new QueryTree.ColumnDef(parts[1], parts[3])); // indexName, columnName
        }
        return query;
    }
    
    private QueryTree analyzeDropTable(ASTNode ast) {
        QueryTree query = new QueryTree(QueryTree.Type.DROP_TABLE);
        String tableName = ast.getValue();
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new RuntimeException("Table name is null or empty");
        }
        tableName = tableName.trim();
        query.setTableName(tableName);
        
        if (!storageManager.tableExists(query.getTableName())) {
            throw new RuntimeException("Table does not exist: " + query.getTableName());
        }
        
        return query;
    }
    
    private QueryTree analyzeCreateTable(ASTNode ast) {
        QueryTree query = new QueryTree(QueryTree.Type.CREATE_TABLE);
        String tableName = ast.getValue();
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new RuntimeException("Table name is null or empty");
        }
        tableName = tableName.trim();
        // Проверяем, что это не ключевое слово или символ
        if (tableName.equals("(") || tableName.equals("VALUES") || tableName.equals("WHERE") || 
            tableName.equals("FROM") || tableName.equals("SELECT") || tableName.equals("INSERT")) {
            throw new RuntimeException("Invalid table name (reserved word): " + tableName);
        }
        query.setTableName(tableName);
        
        if (storageManager.tableExists(query.getTableName())) {
            throw new RuntimeException("Table already exists: " + query.getTableName());
        }
        
        for (ASTNode child : ast.getChildren()) {
            if (child.getType() == ASTNode.Type.COLUMN_DEF) {
                String colName = child.getChildren().get(0).getValue();
                String colType = child.getChildren().get(1).getValue();
                query.getColumns().add(new QueryTree.ColumnDef(colName, colType));
            }
        }
        
        return query;
    }
    
    private QueryTree analyzeInsert(ASTNode ast) {
        QueryTree query = new QueryTree(QueryTree.Type.INSERT);
        String tableName = ast.getValue();
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new RuntimeException("Table name is null or empty");
        }
        tableName = tableName.trim();
        // Проверяем, что это не ключевое слово
        if (tableName.equals("VALUES") || tableName.equals("WHERE") || tableName.equals("(") ||
            tableName.equals("FROM") || tableName.equals("SELECT") || tableName.equals("INSERT")) {
            throw new RuntimeException("Invalid table name (reserved word): " + tableName);
        }
        query.setTableName(tableName);
        
        if (!storageManager.tableExists(query.getTableName())) {
            throw new RuntimeException("Table does not exist: " + query.getTableName());
        }
        
        TableMetadata metadata = storageManager.getTableMetadata(query.getTableName());
        
        // Получаем список колонок (если указан) или используем все колонки
        List<String> columnNames = new java.util.ArrayList<>();
        ASTNode valuesNode = null;
        
        // Определяем структуру: если два COLUMN_LIST, первый - колонки, второй - значения
        // Если один COLUMN_LIST, это значения (колонок нет в AST)
        if (ast.getChildren().size() == 2 && 
            ast.getChildren().get(0).getType() == ASTNode.Type.COLUMN_LIST &&
            ast.getChildren().get(1).getType() == ASTNode.Type.COLUMN_LIST) {
            // Явный список колонок
            ASTNode columnListNode = ast.getChildren().get(0);
            for (ASTNode colNode : columnListNode.getChildren()) {
                if (colNode.getType() != ASTNode.Type.IDENTIFIER) {
                    throw new RuntimeException("Invalid column name in INSERT column list: " + colNode.getType());
                }
                columnNames.add(colNode.getValue());
            }
            valuesNode = ast.getChildren().get(1);
        } else if (ast.getChildren().size() == 1 &&
                   ast.getChildren().get(0).getType() == ASTNode.Type.COLUMN_LIST) {
            // Нет явного списка колонок, только значения
            for (TableMetadata.Column col : metadata.getColumns()) {
                columnNames.add(col.getName());
            }
            valuesNode = ast.getChildren().get(0);
        } else {
            throw new RuntimeException("Invalid INSERT AST structure: expected 1 or 2 COLUMN_LIST children, got: " + ast.getChildren().size());
        }
        
        if (valuesNode == null) {
            throw new RuntimeException("Missing VALUES clause for INSERT");
        }
        
        List<Object> values = new java.util.ArrayList<>();
        for (ASTNode valueNode : valuesNode.getChildren()) {
            Object value = extractValue(valueNode);
            values.add(value);
        }
        
        if (values.size() != columnNames.size()) {
            throw new RuntimeException("Column count mismatch");
        }
        
        // Проверяем существование колонок
        for (String colName : columnNames) {
            if (metadata.getColumn(colName) == null) {
                throw new RuntimeException("Column does not exist: " + colName);
            }
        }
        
        query.getInsertValues().addAll(values);
        return query;
    }
    
    private QueryTree analyzeSelect(ASTNode ast) {
        QueryTree query = new QueryTree(QueryTree.Type.SELECT);
        
        // Таблица
        String tableName = null;
        ASTNode whereClause = null;
        for (ASTNode child : ast.getChildren()) {
            if (child.getType() == ASTNode.Type.IDENTIFIER && tableName == null) {
                tableName = child.getValue();
            } else if (child.getType() == ASTNode.Type.WHERE_CLAUSE) {
                whereClause = child;
            } else if (child.getType() == ASTNode.Type.COLUMN_LIST) {
                for (ASTNode colNode : child.getChildren()) {
                    query.getSelectColumns().add(colNode.getValue());
                }
            }
        }
        
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new RuntimeException("Table name is null or empty");
        }
        tableName = tableName.trim();
        // Проверяем, что это не ключевое слово
        if (tableName.equals("WHERE") || tableName.equals("VALUES") || tableName.equals("(") ||
            tableName.equals("FROM") || tableName.equals("SELECT") || tableName.equals("INSERT")) {
            throw new RuntimeException("Invalid table name (reserved word): " + tableName);
        }
        query.setTableName(tableName);
        
        if (!storageManager.tableExists(query.getTableName())) {
            throw new RuntimeException("Table does not exist: " + query.getTableName());
        }
        
        TableMetadata metadata = storageManager.getTableMetadata(tableName);
        
        // Проверяем колонки
        if (query.getSelectColumns().contains("*")) {
            query.getSelectColumns().clear();
            for (TableMetadata.Column col : metadata.getColumns()) {
                query.getSelectColumns().add(col.getName());
            }
        } else {
            for (String colName : query.getSelectColumns()) {
                if (metadata.getColumn(colName) == null) {
                    throw new RuntimeException("Column does not exist: " + colName);
                }
            }
        }
        
        // WHERE условие
        if (whereClause != null && !whereClause.getChildren().isEmpty()) {
            query.setWhereCondition(analyzeExpression(whereClause.getChildren().get(0), metadata));
        }
        
        return query;
    }
    
    private QueryTree.Expression analyzeExpression(ASTNode ast, TableMetadata metadata) {
        if (ast.getType() == ASTNode.Type.BINARY_OP) {
            String op = ast.getValue();
            if (op == null) {
                throw new RuntimeException("Binary operator is null");
            }
            
            // Проверяем, что это действительно оператор, а не число или что-то другое
            String opTrimmed = op.trim();
            if (!opTrimmed.equals("=") && !opTrimmed.equals("<>") && !opTrimmed.equals("<") && !opTrimmed.equals("<=") && 
                !opTrimmed.equals(">") && !opTrimmed.equals(">=") && !opTrimmed.equals("AND") && !opTrimmed.equals("OR")) {
                // Если это не оператор, возможно AST построен неправильно
                throw new RuntimeException("Invalid operator in BINARY_OP node: '" + op + 
                    "'. Expected one of: =, <>, <, <=, >, >=, AND, OR. " +
                    "AST children: " + ast.getChildren().size() + 
                    (ast.getChildren().size() > 0 ? ", left=" + ast.getChildren().get(0).getType() + "(" + ast.getChildren().get(0).getValue() + ")" : "") +
                    (ast.getChildren().size() > 1 ? ", right=" + ast.getChildren().get(1).getType() + "(" + ast.getChildren().get(1).getValue() + ")" : ""));
            }
            
            QueryTree.Expression.OpType opType;
            try {
                opType = parseOpType(opTrimmed);
            } catch (RuntimeException e) {
                throw new RuntimeException("Failed to parse operator '" + op + "': " + e.getMessage(), e);
            }
            
            if (ast.getChildren().size() < 2) {
                throw new RuntimeException("Binary operator requires 2 operands, got: " + ast.getChildren().size());
            }
            
            ASTNode left = ast.getChildren().get(0);
            ASTNode right = ast.getChildren().get(1);
            
            if (opType == QueryTree.Expression.OpType.AND || 
                opType == QueryTree.Expression.OpType.OR) {
                return new QueryTree.Expression(opType, 
                    analyzeExpression(left, metadata),
                    analyzeExpression(right, metadata));
            } else {
                // Оператор сравнения
                String colName = null;
                Object value = null;
                
                if (left.getType() == ASTNode.Type.IDENTIFIER) {
                    colName = left.getValue();
                    value = extractValue(right);
                } else if (right.getType() == ASTNode.Type.IDENTIFIER) {
                    colName = right.getValue();
                    value = extractValue(left);
                } else {
                    // Оба операнда не идентификаторы - ошибка
                    throw new RuntimeException("Invalid expression: expected column name, got left=" + 
                        left.getType() + " right=" + right.getType());
                }
                
                if (colName == null || colName.trim().isEmpty()) {
                    throw new RuntimeException("Invalid expression: column name is null or empty");
                }
                
                if (metadata.getColumn(colName) == null) {
                    throw new RuntimeException("Column does not exist: " + colName);
                }
                
                return new QueryTree.Expression(opType, colName, value);
            }
        } else if (ast.getType() == ASTNode.Type.IDENTIFIER || ast.getType() == ASTNode.Type.LITERAL) {
            // Простое значение или идентификатор - не валидное выражение для WHERE
            throw new RuntimeException("Invalid expression: expected binary operator, got: " + ast.getType());
        }
        
        throw new RuntimeException("Invalid expression type: " + ast.getType());
    }
    
    private QueryTree.Expression.OpType parseOpType(String op) {
        switch (op) {
            case "=": return QueryTree.Expression.OpType.EQ;
            case "<>": return QueryTree.Expression.OpType.NE;
            case "<": return QueryTree.Expression.OpType.LT;
            case "<=": return QueryTree.Expression.OpType.LE;
            case ">": return QueryTree.Expression.OpType.GT;
            case ">=": return QueryTree.Expression.OpType.GE;
            case "AND": return QueryTree.Expression.OpType.AND;
            case "OR": return QueryTree.Expression.OpType.OR;
            default:
                throw new RuntimeException("Unknown operator: " + op);
        }
    }
    
    private Object extractValue(ASTNode node) {
        if (node.getType() == ASTNode.Type.LITERAL) {
            String value = node.getValue();
            if (value == null) {
                throw new RuntimeException("Literal value is null");
            }
            // Пытаемся определить тип
            if (value.startsWith("'") && value.endsWith("'") && value.length() > 1) {
                return value.substring(1, value.length() - 1);
            }
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                return value;
            }
        } else if (node.getType() == ASTNode.Type.IDENTIFIER) {
            // Если это идентификатор, возможно это имя колонки (не должно быть в VALUES)
            // Но на всякий случай вернем как строку
            throw new RuntimeException("Unexpected identifier in value: " + node.getValue());
        } else if (node.getType() == ASTNode.Type.BINARY_OP) {
            // Это выражение, а не простое значение - ошибка
            throw new RuntimeException("Complex expression not supported in VALUES clause: " + node.getValue());
        }
        throw new RuntimeException("Expected literal value, got: " + node.getType() + " (" + node.getValue() + ")");
    }
}

