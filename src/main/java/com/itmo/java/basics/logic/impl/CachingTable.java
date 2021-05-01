package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.DatabaseCache;
import com.itmo.java.basics.logic.Table;

import java.util.Optional;

/**
 * Декторато для таблицы. Кэширует данные
 */
public class CachingTable implements Table {
    private final Table table;
    private final DatabaseCache cache;

    public CachingTable(Table table) {
        this.table = table;
        this.cache = new DatabaseCacheImpl();
    }

    @Override
    public String getName() {
        return table.getName();
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        cache.set(objectKey, objectValue);
        table.write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        byte[] value = cache.get(objectKey);
        if (value == null) {
            // Cache miss
            Optional<byte[]> objectValue = table.read(objectKey);
            objectValue.ifPresent(bytes -> cache.set(objectKey, bytes));
            return objectValue;
        }

        return Optional.of(value);
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        cache.delete(objectKey);
        table.delete(objectKey);
    }
}
