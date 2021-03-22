package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class TableImpl implements Table {
    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        if (new File(pathToDatabaseRoot.toString(), tableName).exists()) {
            throw new DatabaseException("Table already exists");
        }

        return new TableImpl(tableName, pathToDatabaseRoot, tableIndex);
    }

    private final String tableName;
    private final TableIndex tableIndex;
    private final Path tableRootPath;
    private Segment lastCreatedSegment;

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        this.tableName = tableName;
        this.tableIndex = tableIndex;
        this.tableRootPath = Paths.get(pathToDatabaseRoot.toString(), tableName);

        try {
            Files.createDirectory(tableRootPath);
        } catch (IOException e) {
            throw new DatabaseException("Cannot create directory for a table", e);
        }

        this.lastCreatedSegment = SegmentImpl.create(
                SegmentImpl.createSegmentName(tableName), tableRootPath
        );
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        if (objectValue == null) { delete(objectKey); }
        try {
            if (!lastCreatedSegment.write(objectKey, objectValue)) {
                lastCreatedSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), tableRootPath);
                lastCreatedSegment.write(objectKey, objectValue);
            }
        } catch (IOException e) {
            throw new DatabaseException("Writing: Cannot write value to segment", e);
        }

        tableIndex.onIndexedEntityUpdated(objectKey, lastCreatedSegment);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        Optional<Segment> optionalSegment = tableIndex.searchForKey(objectKey);
        if (optionalSegment.isEmpty()) {
            return Optional.empty();
        }

        try {
            return optionalSegment.get().read(objectKey);
        } catch (IOException e) {
            throw new DatabaseException("Cannot read key from segment", e);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        try {
            if (!lastCreatedSegment.delete(objectKey)) {
                lastCreatedSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), tableRootPath);
                lastCreatedSegment.delete(objectKey);
            }
        } catch (IOException e) {
            throw new DatabaseException("Deleting: Cannot write value to segment", e);
        }

        tableIndex.onIndexedEntityUpdated(objectKey, lastCreatedSegment);
    }
}
