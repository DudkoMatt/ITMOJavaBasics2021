package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;
import java.nio.charset.StandardCharsets;

public class RemoveDatabaseRecord implements WritableDatabaseRecord {

    private final String key;

    public RemoveDatabaseRecord(String key) {
        this.key = key;
    }

    @Override
    public byte[] getKey() {
        return key.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getValue() {
        return new byte[0];
    }

    @Override
    public long size() {
        return 2 * 4 + getKeySize();
    }

    @Override
    public boolean isValuePresented() {
        return false;
    }

    @Override
    public int getKeySize() {
        return key.length();
    }

    @Override
    public int getValueSize() {
        return -1;
    }
}
