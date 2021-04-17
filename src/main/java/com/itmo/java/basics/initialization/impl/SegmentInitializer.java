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
import java.util.HashSet;

public class SegmentInitializer implements Initializer {
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
        SegmentIndex segmentIndex = new SegmentIndex();
        HashSet<String> presentKeys = new HashSet<>();

        try (DataInputStream dataInputStream = new DataInputStream(new FileInputStream(context.currentSegmentContext().getSegmentPath().toFile()))) {
            long currentStreamPosition = 0;
            while (dataInputStream.available() > 0) {
                int keySize = dataInputStream.readInt();
                String lastKey = new String(dataInputStream.readNBytes(keySize));
                int valueSize = dataInputStream.readInt();

                if (valueSize != -1) {
                    presentKeys.add(lastKey);
                    dataInputStream.skip(valueSize);
                }

                segmentIndex.onIndexedEntityUpdated(lastKey, new SegmentOffsetInfoImpl(currentStreamPosition));
                currentStreamPosition += 4 * 2 + keySize + (valueSize == -1 ? 0 : valueSize);
            }
        } catch (IOException e) {
            throw new DatabaseException("Cannot create FileInputStream", e);
        }

        Segment segment = SegmentImpl.initializeFromContext(new SegmentInitializationContextImpl(
                context.currentSegmentContext().getSegmentName(),
                context.currentSegmentContext().getSegmentPath(),
                context.currentSegmentContext().getCurrentSize(),
                segmentIndex
        ));

        context.currentTableContext().updateCurrentSegment(segment);

        for (String key: presentKeys) {
            context.currentTableContext().getTableIndex().onIndexedEntityUpdated(key, segment);
        }
    }
}
