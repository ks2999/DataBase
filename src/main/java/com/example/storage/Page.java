package com.example.storage;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Страница данных - базовая единица хранения
 */
public class Page implements Serializable {
    public static final int PAGE_SIZE = 4096; // 4KB
    private static final long serialVersionUID = 1L;
    
    private int pageId;
    private boolean dirty;
    private byte[] data;
    private int freeSpace;
    
    public Page(int pageId) {
        this.pageId = pageId;
        this.data = new byte[PAGE_SIZE];
        this.freeSpace = PAGE_SIZE;
        this.dirty = false;
    }
    
    public int getPageId() {
        return pageId;
    }
    
    public boolean isDirty() {
        return dirty;
    }
    
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }
    
    public byte[] getData() {
        return data;
    }
    
    public int getFreeSpace() {
        return freeSpace;
    }
    
    public void setFreeSpace(int freeSpace) {
        this.freeSpace = freeSpace;
    }
    
    public ByteBuffer getBuffer() {
        return ByteBuffer.wrap(data);
    }
}

