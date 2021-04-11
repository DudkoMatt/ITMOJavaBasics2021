package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.impl.SegmentImpl;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;

// ToDO
public class SegmentInitializer implements Initializer {
    // ToDO: взято из DatabaseInputStream
    private static final int REMOVED_OBJECT_SIZE = -1;

    /**
     * Добавляет в контекст информацию об инициализируемом сегменте.
     * Составляет индекс сегмента
     * Обновляет инфу в индексе таблицы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к сегменту, невозможно прочитать содержимое. Ошибка в содержании
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        Segment segment = SegmentImpl.initializeFromContext(new SegmentInitializationContextImpl(
                context.currentSegmentContext().getSegmentName(),
                context.currentSegmentContext().getSegmentPath(),
                context.currentSegmentContext().getCurrentSize(),
                createIndex(context.currentSegmentContext().getSegmentPath())
        ));

        context.currentTableContext().updateCurrentSegment(segment);
        context.currentTableContext().getTableIndex().onIndexedEntityUpdated(segment.getName(), segment);
    }

    // ToDO: think about refactor -> REMOVED_OBJECT_SIZE should be used from DatabaseInputStream??
    private static SegmentIndex createIndex(Path filepath) throws DatabaseException {
        SegmentIndex segmentIndex = new SegmentIndex();
        try {
            DataInputStream inputStream = new DataInputStream(new FileInputStream(filepath.toString()));
            long currentPosition = 0;

            while (inputStream.available() > 0) {
                int keySize = inputStream.readInt();
                byte[] keyObject = inputStream.readNBytes(keySize);
                int valueSize = inputStream.readInt();
                currentPosition += 2 * 4 + keySize;
                if (valueSize != REMOVED_OBJECT_SIZE) {
                    segmentIndex.onIndexedEntityUpdated(new String(keyObject), new SegmentOffsetInfoImpl(currentPosition));
                    inputStream.skipBytes(valueSize);
                    currentPosition += valueSize;
                }
            }
        } catch (IOException e) {
            throw new DatabaseException("Cannot read file", e);
        }

        return segmentIndex;
    }
}
