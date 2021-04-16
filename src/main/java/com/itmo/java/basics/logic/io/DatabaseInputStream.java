package com.itmo.java.basics.logic.io;

import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.impl.SetDatabaseRecord;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

/**
 * Класс, отвечающий за чтение данных из БД
 */
public class DatabaseInputStream extends DataInputStream {
    private static final int REMOVED_OBJECT_SIZE = -1;
    private long readBytes = 0;
    private byte[] lastKeyObject = null;

    public DatabaseInputStream(InputStream inputStream) {
        super(inputStream);
    }

    /**
     * Читает следующую запись (см {@link DatabaseOutputStream#write(WritableDatabaseRecord)})
     * @return следующую запись, если она существует. {@link Optional#empty()} - если конец файла достигнут
     */
    public Optional<DatabaseRecord> readDbUnit() throws IOException {
        int keySize = readInt();
        byte[] keyObject = readNBytes(keySize);
        int valueSize = readInt();
        readBytes += 4 * 2 + keyObject.length;
        lastKeyObject = keyObject;
        if (valueSize == REMOVED_OBJECT_SIZE) {
            return Optional.empty();
        } else {
            readBytes += valueSize;
            return Optional.of(new SetDatabaseRecord(new String(keyObject), readNBytes(valueSize)));
        }
    }

    public long getReadBytes() {
        return readBytes;
    }

    public byte[] getLastKeyObject() {
        return lastKeyObject;
    }
}
