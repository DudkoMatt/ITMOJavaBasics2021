package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.index.SegmentOffsetInfo;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.logic.DatabaseRecord;
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

public class SegmentImpl implements Segment {
    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        if (new File(tableRootPath.toString(), segmentName).exists())
            throw new DatabaseException("Segment already exists");

        return new SegmentImpl(segmentName, tableRootPath);
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    private static final int MAX_SIZE_IN_BYTES = 100_000;

    private final String segmentName;
    private final Path tableRootPath;
    private int bytesWritten;

    private final SegmentIndex segmentIndex;
    private final DatabaseOutputStream dataOutputStream;

    private SegmentImpl(String segmentName, Path tableRootPath) throws DatabaseException {
        this.segmentName = segmentName;
        this.tableRootPath = tableRootPath;
        this.bytesWritten = 0;

        this.segmentIndex = new SegmentIndex();

        Path fullFilePath = Paths.get(tableRootPath.toString(), segmentName);

        try {
            if (!Files.exists(fullFilePath)) {
                Files.createFile(fullFilePath);
            }
        } catch (IOException e) {
            throw new DatabaseException("Cannot create a segment", e);
        }

        try {
            this.dataOutputStream = new DatabaseOutputStream(Files.newOutputStream(fullFilePath, APPEND));
        } catch (IOException e) {
            throw new DatabaseException("Cannot create an I/O stream", e);
        }
    }

    @Override
    public String getName() {
        return segmentName;
    }

    private boolean writeToFile(WritableDatabaseRecord databaseRecord) throws IOException {
        if (isReadOnly()) return false;
        dataOutputStream.write(databaseRecord);

        segmentIndex.onIndexedEntityUpdated(new String(databaseRecord.getKey()), new SegmentOffsetInfoImpl(bytesWritten));
        bytesWritten += databaseRecord.size();

        dataOutputStream.flush();

        if (isReadOnly())
            dataOutputStream.close();

        return true;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {
        return writeToFile(new SetDatabaseRecord(objectKey, objectValue));
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        Optional<SegmentOffsetInfo> optionalSegmentOffsetInfo = segmentIndex.searchForKey(objectKey);

        if (optionalSegmentOffsetInfo.isEmpty())
            return Optional.empty();

        SegmentOffsetInfo offsetInfo = optionalSegmentOffsetInfo.get();

        DatabaseInputStream dataInputStream = new DatabaseInputStream(
                Files.newInputStream(Paths.get(tableRootPath.toString(), segmentName))
        );

        long skipped = dataInputStream.skip(offsetInfo.getOffset());
        if (skipped != offsetInfo.getOffset())
            throw new IOException("Cannot skip offset");

        Optional<DatabaseRecord> optionalDatabaseRecord = dataInputStream.readDbUnit();

        if (optionalDatabaseRecord.isEmpty())
            return Optional.empty();

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
