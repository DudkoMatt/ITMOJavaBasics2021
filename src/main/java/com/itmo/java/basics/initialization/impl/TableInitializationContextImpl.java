package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Segment;

import java.nio.file.Path;
import java.nio.file.Paths;

public class TableInitializationContextImpl implements TableInitializationContext {
    private final String tableName;
    private final Path databasePath;
    private final TableIndex tableIndex;
    private Segment currentSegment;

    public TableInitializationContextImpl(String tableName, Path databasePath, TableIndex tableIndex) {
        this.tableName = tableName;
        this.databasePath = databasePath;
        this.tableIndex = tableIndex;
        this.currentSegment = null;
    }

    public TableInitializationContextImpl(String tableName, Path databasePath, TableIndex tableIndex, Segment currentSegment) {
        this(tableName, databasePath, tableIndex);
        this.currentSegment = currentSegment;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public Path getTablePath() {
        return Paths.get(databasePath.toString(), tableName);
    }

    @Override
    public TableIndex getTableIndex() {
        return tableIndex;
    }

    @Override
    public Segment getCurrentSegment() {
        return currentSegment;
    }

    @Override
    public void updateCurrentSegment(Segment segment) {
        currentSegment = segment;
        tableIndex.onIndexedEntityUpdated(segment.getName(), segment);
    }
}
