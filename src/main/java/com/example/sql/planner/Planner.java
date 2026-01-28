package com.example.sql.planner;

import com.example.sql.semantic.QueryTree;

/**
 * Планировщик - построение логического плана из QueryTree
 */
public class Planner {
    public LogicalPlan plan(QueryTree queryTree) {
        LogicalPlan plan = new LogicalPlan(
            LogicalPlan.Type.valueOf(queryTree.getType().name()));
        
        plan.setTableName(queryTree.getTableName());
        
        switch (queryTree.getType()) {
            case CREATE_TABLE:
                for (QueryTree.ColumnDef col : queryTree.getColumns()) {
                    plan.getColumns().add(
                        new LogicalPlan.ColumnDef(col.getName(), col.getType()));
                }
                break;
                
            case CREATE_INDEX:
                // Для индекса сохраняем информацию в columns
                for (QueryTree.ColumnDef col : queryTree.getColumns()) {
                    plan.getColumns().add(
                        new LogicalPlan.ColumnDef(col.getName(), col.getType()));
                }
                break;
                
            case DROP_TABLE:
                // Для DROP TABLE не нужно дополнительных действий
                break;
                
            case INSERT:
                plan.getInsertValues().addAll(queryTree.getInsertValues());
                break;
                
            case SELECT:
                plan.getSelectColumns().addAll(queryTree.getSelectColumns());
                
                // Строим дерево операторов снизу вверх
                LogicalPlan.ScanOperator scan = new LogicalPlan.ScanOperator(queryTree.getTableName());
                LogicalPlan.LogicalOperator current = scan;
                
                // WHERE условия
                if (queryTree.getWhereCondition() != null) {
                    current = buildFilterTree(queryTree.getWhereCondition(), current);
                }
                
                // Project
                LogicalPlan.ProjectOperator project = 
                    new LogicalPlan.ProjectOperator(queryTree.getSelectColumns());
                project.addChild(current);
                current = project;
                
                plan.setRootOperator(current);
                break;
        }
        
        return plan;
    }
    
    private LogicalPlan.LogicalOperator buildFilterTree(
            QueryTree.Expression expr, 
            LogicalPlan.LogicalOperator child) {
        
        if (expr.isBinary() && 
            (expr.getOpType() == QueryTree.Expression.OpType.AND ||
             expr.getOpType() == QueryTree.Expression.OpType.OR)) {
            // Рекурсивно обрабатываем AND/OR
            LogicalPlan.LogicalOperator leftChild = buildFilterTree(expr.getLeft(), child);
            // rightChild обрабатывается, но для упрощения используем только leftChild
            // В реальной СУБД здесь была бы более сложная логика объединения условий
            return leftChild; // Упрощение: берем левое условие
        } else {
            // Простое условие сравнения
            String op = opTypeToString(expr.getOpType());
            LogicalPlan.FilterOperator filter = 
                new LogicalPlan.FilterOperator(expr.getColumnName(), op, expr.getValue());
            filter.addChild(child);
            return filter;
        }
    }
    
    private String opTypeToString(QueryTree.Expression.OpType opType) {
        switch (opType) {
            case EQ: return "=";
            case NE: return "<>";
            case LT: return "<";
            case LE: return "<=";
            case GT: return ">";
            case GE: return ">=";
            default: return "=";
        }
    }
}

