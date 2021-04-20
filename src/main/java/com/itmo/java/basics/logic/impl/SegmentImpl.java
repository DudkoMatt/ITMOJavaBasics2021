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
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static java.nio.file.StandardOpenOption.APPEND;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
public class SegmentImpl implements Segment {
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

    public static Segment initializeFromContext(SegmentInitializationContext context) {
        return SegmentImpl.builder()
                .segmentName(context.getSegmentName())
                .tableRootPath(context.getSegmentPath().getParent())
                .segmentIndex(context.getIndex())
                .bytesWritten(context.getCurrentSize())
                .build();
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    private static final int MAX_SIZE_IN_BYTES = 100_000;

    private final String segmentName;
    private final Path tableRootPath;
    private long bytesWritten;

    private final SegmentIndex segmentIndex;

    private SegmentImpl(String segmentName, Path tableRootPath, SegmentIndex segmentIndex) {
        this.segmentName = segmentName;
        this.tableRootPath = tableRootPath;
        this.bytesWritten = 0;

        this.segmentIndex = segmentIndex;
    }

    @Override
    public String getName() {
        return segmentName;
    }

    private boolean writeToFile(WritableDatabaseRecord databaseRecord) throws IOException {
        if (isReadOnly()) {
            return false;
        }

        DatabaseOutputStream dataOutputStream = new DatabaseOutputStream(Files.newOutputStream(Paths.get(tableRootPath.toString(), segmentName), APPEND));

        dataOutputStream.write(databaseRecord);

        segmentIndex.onIndexedEntityUpdated(new String(databaseRecord.getKey()), new SegmentOffsetInfoImpl(bytesWritten));
        bytesWritten += databaseRecord.size();

        dataOutputStream.flush();
        dataOutputStream.close();

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
