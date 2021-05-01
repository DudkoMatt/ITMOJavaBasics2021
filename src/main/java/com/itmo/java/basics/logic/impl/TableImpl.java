package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Таблица - логическая сущность, представляющая собой набор файлов-сегментов, которые объединены одним
 * именем и используются для хранения однотипных данных (данных, представляющих собой одну и ту же сущность,
 * например, таблица "Пользователи")
 * <p>
 * - имеет единый размер сегмента
 * - представляет из себя директорию в файловой системе, именованную как таблица
 * и хранящую файлы-сегменты данной таблицы
 */
public class TableImpl implements Table {
    private final String tableName;
    private final Path tableRootPath;
    private final TableIndex tableIndex;
    private Segment lastCreatedSegment;

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        this.tableName = tableName;
        this.tableRootPath = Paths.get(pathToDatabaseRoot.toString(), tableName);
        this.tableIndex = tableIndex;

        this.lastCreatedSegment = SegmentImpl.create(
                SegmentImpl.createSegmentName(tableName), tableRootPath
        );
    }

    private TableImpl(String tableName, Path tableRootPath,  TableIndex tableIndex, Segment lastCreatedSegment) {
        this.tableName = tableName;
        this.tableRootPath = tableRootPath;
        this.tableIndex = tableIndex;
        this.lastCreatedSegment = lastCreatedSegment;
    }

    public static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        if (new File(pathToDatabaseRoot.toString(), tableName).exists()) {
            throw new DatabaseException("Table already exists");
        }

        try {
            Files.createDirectory(Paths.get(pathToDatabaseRoot.toString(), tableName));
        } catch (IOException e) {
            throw new DatabaseException("Cannot create directory for a table", e);
        }

        return new CachingTable(new TableImpl(tableName, pathToDatabaseRoot, tableIndex));
    }

    public static Table initializeFromContext(TableInitializationContext context) {
        if (context.getCurrentSegment() == null) {
            try {
                context.updateCurrentSegment(SegmentImpl.create(SegmentImpl.createSegmentName(context.getTableName()), context.getTablePath().getParent()));
            } catch (DatabaseException e) {
                throw new RuntimeException("Cannot create new segment during initialization", e);
            }
        }

        return new CachingTable(new TableImpl(context.getTableName(), context.getTablePath(), context.getTableIndex(), context.getCurrentSegment()));
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        if (objectValue == null) {
            delete(objectKey);
        } else {
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
