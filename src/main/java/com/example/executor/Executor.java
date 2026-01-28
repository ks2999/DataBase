package com.example.executor;

/**
 * Базовый интерфейс executor в стиле Volcano
 */
public interface Executor {
    /**
     * Открыть executor и подготовить к выполнению
     */
    void open();
    
    /**
     * Получить следующую строку (null если больше нет)
     */
    Row next();
    
    /**
     * Закрыть executor и освободить ресурсы
     */
    void close();
}

