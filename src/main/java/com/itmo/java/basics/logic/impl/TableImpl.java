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
        if (new File(pathToDatabaseRoot.toString(), tableName).exists())
            throw new DatabaseException("Table already exists");

        return new TableImpl(tableName, pathToDatabaseRoot, tableIndex);
    }

    private final String tableName;
    private final TableIndex tableIndex;
    private final Path pathToDatabaseRoot;
    private Segment lastCreatedSegment;

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        this.tableName = tableName;
        this.tableIndex = tableIndex;
        this.pathToDatabaseRoot = pathToDatabaseRoot;

        this.lastCreatedSegment = SegmentImpl.create(
                SegmentImpl.createSegmentName(tableName), Paths.get(pathToDatabaseRoot.toString(), tableName)
        );

        try {
            Files.createDirectory(Paths.get(pathToDatabaseRoot.toString(), tableName));
        } catch (IOException e) {
            throw new DatabaseException("Cannot create directory for a table", e);
        }
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        // Берем последний индекс и пытаемся записать туда значение, если он не заполнен
        if (lastCreatedSegment.isReadOnly())
            lastCreatedSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), pathToDatabaseRoot);

        try {
            lastCreatedSegment.write(objectKey, objectValue);
        } catch (IOException e) {
            throw new DatabaseException("Cannot write value to segment", e);
        }

        tableIndex.onIndexedEntityUpdated(objectKey, lastCreatedSegment);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        Optional<Segment> optionalSegment = tableIndex.searchForKey(objectKey);
        if (optionalSegment.isEmpty()) {
            throw new DatabaseException("Cannot find key in segment");
        }

        try {
            return optionalSegment.get().read(objectKey);
        } catch (IOException e) {
            throw new DatabaseException("Cannot read key from segment", e);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        Optional<Segment> optionalSegment = tableIndex.searchForKey(objectKey);
        if (optionalSegment.isEmpty()) {
            throw new DatabaseException("Cannot find key in segment");
        }

        try {
            optionalSegment.get().delete(objectKey);
        } catch (IOException e) {
            throw new DatabaseException("Cannot delete key from segment", e);
        }
    }
}
