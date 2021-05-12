package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.index.SegmentOffsetInfo;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static java.nio.file.StandardOpenOption.APPEND;

/**
 * Сегмент - append-only файл, хранящий пары ключ-значение, разделенные специальным символом.
 * - имеет ограниченный размер, большие значения (>100000) записываются в последний сегмент, если он не read-only
 * - при превышении размера сегмента создается новый сегмент и дальнейшие операции записи производятся в него
 * - именование файла-сегмента должно позволять установить очередность их появления
 * - является неизменяемым после появления более нового сегмента
 */
public class SegmentImpl implements Segment {
    private static final int MAX_SIZE_IN_BYTES = 100_000;

    private final String segmentName;
    private final Path tableRootPath;
    private long bytesWritten;

    private final SegmentIndex segmentIndex;

    private SegmentImpl(String segmentName, Path tableRootPath, SegmentIndex segmentIndex) {
        this.segmentName = segmentName;
        this.tableRootPath = tableRootPath;
        this.segmentIndex = segmentIndex;
        this.bytesWritten = 0;
    }

    private SegmentImpl(String segmentName, Path tableRootPath, SegmentIndex segmentIndex, long bytesWritten) {
        this(segmentName, tableRootPath, segmentIndex);
        this.bytesWritten = bytesWritten;
    }

    public static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        if (new File(tableRootPath.toString(), segmentName).exists()) {
            throw new DatabaseException("Segment already exists");
        }

        Path fullSegmentPath = Paths.get(tableRootPath.toString(), segmentName);

        try {
            if (!Files.exists(fullSegmentPath)) {
                Files.createFile(fullSegmentPath);
            }
        } catch (IOException e) {
            throw new DatabaseException("Cannot create a segment", e);
        }

        return new SegmentImpl(segmentName, tableRootPath, new SegmentIndex());
    }

    static String createSegmentName(String tableName) {
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted Thread.sleep", e);
        }

        return tableName + "_" + System.currentTimeMillis();
    }

    public static Segment initializeFromContext(SegmentInitializationContext context) {
        return new SegmentImpl(context.getSegmentName(), context.getSegmentPath().getParent(), context.getIndex(), context.getCurrentSize());
    }

    @Override
    public String getName() {
        return segmentName;
    }

    private boolean writeToFile(WritableDatabaseRecord databaseRecord) throws IOException {
        if (isReadOnly()) {
            return false;
        }

        try (DatabaseOutputStream dataOutputStream = new DatabaseOutputStream(Files.newOutputStream(Paths.get(tableRootPath.toString(), segmentName), APPEND))) {
            dataOutputStream.write(databaseRecord);

            segmentIndex.onIndexedEntityUpdated(new String(databaseRecord.getKey()), new SegmentOffsetInfoImpl(bytesWritten));
            bytesWritten += databaseRecord.size();

            dataOutputStream.flush();
        }

        return true;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {
        if (objectValue == null) {
            return delete(objectKey);
        }
        return writeToFile(new SetDatabaseRecord(objectKey, objectValue));
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        Optional<SegmentOffsetInfo> optionalSegmentOffsetInfo = segmentIndex.searchForKey(objectKey);

        if (optionalSegmentOffsetInfo.isEmpty()) {
            return Optional.empty();
        }

        SegmentOffsetInfo offsetInfo = optionalSegmentOffsetInfo.get();

        DatabaseInputStream dataInputStream = new DatabaseInputStream(
                Files.newInputStream(Paths.get(tableRootPath.toString(), segmentName))
        );

        long skipped = dataInputStream.skip(offsetInfo.getOffset());
        if (skipped != offsetInfo.getOffset()) {
            throw new IOException("Cannot skip offset");
        }

        Optional<DatabaseRecord> optionalDatabaseRecord = dataInputStream.readDbUnit();

        if (optionalDatabaseRecord.isEmpty() || !optionalDatabaseRecord.get().isValuePresented()) {
            return Optional.empty();
        }

        return Optional.of(optionalDatabaseRecord.get().getValue());
    }

    @Override
    public boolean isReadOnly() {
        return bytesWritten >= MAX_SIZE_IN_BYTES;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {
        return writeToFile(new RemoveDatabaseRecord(objectKey));
    }
}
