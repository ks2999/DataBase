package com.example.index;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * B+Tree индекс для поддержки поиска и range-сканов
 */
public class BPlusTree {
    private String indexName;
    private String tableName;
    private String columnName;
    private String dataDir;
    private Node root;
    private int order; // Порядок дерева
    
    public BPlusTree(String indexName, String tableName, String columnName, String dataDir) {
        this.indexName = indexName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.dataDir = dataDir;
        this.order = 4; // Минимум 2 ключа на узел
        loadIndex();
    }
    
    private void loadIndex() {
        Path indexPath = Paths.get(dataDir, indexName + ".idx");
        if (Files.exists(indexPath)) {
            try (ObjectInputStream ois = new ObjectInputStream(
                    new FileInputStream(indexPath.toFile()))) {
                this.root = (Node) ois.readObject();
            } catch (Exception e) {
                this.root = new LeafNode();
            }
        } else {
            this.root = new LeafNode();
        }
    }
    
    public void saveIndex() {
        try {
            Files.createDirectories(Paths.get(dataDir));
            Path indexPath = Paths.get(dataDir, indexName + ".idx");
            try (ObjectOutputStream oos = new ObjectOutputStream(
                    new FileOutputStream(indexPath.toFile()))) {
                oos.writeObject(root);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to save index", e);
        }
    }
    
    @SuppressWarnings("unchecked")
    public void insert(Comparable<?> key, int pageId, int slotId) {
        IndexEntry entry = new IndexEntry(key, pageId, slotId);
        InsertResult result = root.insert(entry, order);
        
        if (result != null && result.newNode != null) {
            // Нужно создать новый корень
            InternalNode newRoot = new InternalNode();
            newRoot.keys.add(result.newNode.keys.get(0));
            newRoot.children.add(root);
            newRoot.children.add(result.newNode);
            root = newRoot;
        }
    }
    
    @SuppressWarnings("unchecked")
    public List<IndexEntry> search(Comparable<?> key) {
        return root.search(key);
    }
    
    @SuppressWarnings("unchecked")
    public List<IndexEntry> rangeScan(Comparable<?> start, Comparable<?> end) {
        return root.rangeScan(start, end);
    }
    
    public boolean isEmpty() {
        return root instanceof LeafNode && ((LeafNode) root).entries.isEmpty();
    }
    
    // Базовый класс для узлов
    @SuppressWarnings("rawtypes")
    abstract static class Node implements Serializable {
        List<Comparable> keys = new ArrayList<>();
        
        abstract InsertResult insert(IndexEntry entry, int order);
        @SuppressWarnings("unchecked")
        abstract List<IndexEntry> search(Comparable key);
        @SuppressWarnings("unchecked")
        abstract List<IndexEntry> rangeScan(Comparable start, Comparable end);
        
        @SuppressWarnings({"rawtypes", "unchecked"})
        static int findInsertPosition(List<Comparable> keys, Comparable key) {
            int pos = 0;
            for (int i = 0; i < keys.size(); i++) {
                if (keys.get(i).compareTo(key) > 0) {
                    break;
                }
                pos = i + 1;
            }
            return pos;
        }
    }
    
    // Внутренний узел
    static class InternalNode extends Node {
        List<Node> children = new ArrayList<>();
        
        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        InsertResult insert(IndexEntry entry, int order) {
            int pos = findInsertPosition(keys, entry.key);
            
            InsertResult result = children.get(pos).insert(entry, order);
            
            if (result != null && result.newNode != null) {
                keys.add(pos, result.newNode.keys.get(0));
                children.add(pos + 1, result.newNode);
                
                if (keys.size() > order) {
                    // Разделение
                    int mid = keys.size() / 2;
                    InternalNode newNode = new InternalNode();
                    newNode.keys = new ArrayList<>(keys.subList(mid + 1, keys.size()));
                    newNode.children = new ArrayList<>(children.subList(mid + 1, children.size()));
                    keys = new ArrayList<>(keys.subList(0, mid));
                    children = new ArrayList<>(children.subList(0, mid + 1));
                    return new InsertResult(newNode);
                }
            }
            return null;
        }
        
        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        List<IndexEntry> search(Comparable key) {
            int pos = findInsertPosition(keys, key);
            return children.get(pos).search(key);
        }
        
        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        List<IndexEntry> rangeScan(Comparable start, Comparable end) {
            int pos = findInsertPosition(keys, start);
            return children.get(pos).rangeScan(start, end);
        }
    }
    
    // Листовой узел
    static class LeafNode extends Node {
        List<IndexEntry> entries = new ArrayList<>();
        LeafNode next; // Связь для range-сканов
        
        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        InsertResult insert(IndexEntry entry, int order) {
            int pos = findInsertPosition(keys, entry.key);
            
            keys.add(pos, entry.key);
            entries.add(pos, entry);
            
            if (keys.size() > order) {
                // Разделение
                int mid = keys.size() / 2;
                LeafNode newNode = new LeafNode();
                newNode.keys = new ArrayList<>(keys.subList(mid, keys.size()));
                newNode.entries = new ArrayList<>(entries.subList(mid, entries.size()));
                keys = new ArrayList<>(keys.subList(0, mid));
                entries = new ArrayList<>(entries.subList(0, mid));
                newNode.next = this.next;
                this.next = newNode;
                return new InsertResult(newNode);
            }
            return null;
        }
        
        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        List<IndexEntry> search(Comparable key) {
            List<IndexEntry> result = new ArrayList<>();
            for (int i = 0; i < keys.size(); i++) {
                if (keys.get(i).equals(key)) {
                    result.add(entries.get(i));
                }
            }
            return result;
        }
        
        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        List<IndexEntry> rangeScan(Comparable start, Comparable end) {
            List<IndexEntry> result = new ArrayList<>();
            LeafNode node = this;
            
            while (node != null) {
                for (int i = 0; i < node.keys.size(); i++) {
                    Comparable key = node.keys.get(i);
                    if (key.compareTo(start) >= 0 && key.compareTo(end) <= 0) {
                        result.add(node.entries.get(i));
                    } else if (key.compareTo(end) > 0) {
                        return result;
                    }
                }
                node = node.next;
            }
            return result;
        }
    }
    
    static class InsertResult {
        Node newNode;
        
        InsertResult(Node newNode) {
            this.newNode = newNode;
        }
    }
    
    public static class IndexEntry implements Serializable {
        @SuppressWarnings("rawtypes")
        Comparable key;
        int pageId;
        int slotId;
        
        @SuppressWarnings("rawtypes")
        public IndexEntry(Comparable key, int pageId, int slotId) {
            this.key = key;
            this.pageId = pageId;
            this.slotId = slotId;
        }
        
        @SuppressWarnings("rawtypes")
        public Comparable getKey() {
            return key;
        }
        
        public int getPageId() {
            return pageId;
        }
        
        public int getSlotId() {
            return slotId;
        }
    }
}

