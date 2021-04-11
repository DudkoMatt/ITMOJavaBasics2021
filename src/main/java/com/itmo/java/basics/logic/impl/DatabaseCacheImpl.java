package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCacheImpl implements DatabaseCache {
    private final LRUCache<String, byte[]> cache;

    public DatabaseCacheImpl() {
        this.cache = new LRUCache<>();
    }
    
    public DatabaseCacheImpl(int initialCapacity) {
        this.cache = new LRUCache<>(initialCapacity);
    }

    @Override
    public byte[] get(String key) {
        return cache.get(key);
    }

    @Override
    public void set(String key, byte[] value) {
        cache.put(key, value);
    }

    @Override
    public void delete(String key) {
        cache.remove(key);
    }

    public static class LRUCache<K, V> extends LinkedHashMap<K, V> {
        private int capacity;

        public LRUCache(int initialCapacity) {
            super(initialCapacity, 1f, true);
            this.capacity = initialCapacity;
        }

        public LRUCache() {
            this(5);
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > capacity;
        }

        public void setCapacity(int capacity) {
            this.capacity = capacity;
        }
    }
}
