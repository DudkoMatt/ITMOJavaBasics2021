package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.nio.charset.StandardCharsets;

public class SetDatabaseRecord implements WritableDatabaseRecord {

    private final String key;
    private final byte[] value;

    public SetDatabaseRecord(String key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public byte[] getKey() {
        return key.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public long size() {
        return 2 * 4 + getKeySize() + getValueSize();
    }

    @Override
    public boolean isValuePresented() {
        return true;
    }

    @Override
    public int getKeySize() {
        return key.length();
    }

    @Override
    public int getValueSize() {
        return value.length;
    }
}
