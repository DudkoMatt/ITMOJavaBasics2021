package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.SegmentOffsetInfo;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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
        HashMap<String, SegmentOffsetInfo> presentKeys = new HashMap<>();

        try (DatabaseInputStream databaseInputStream = new DatabaseInputStream(new FileInputStream(context.currentSegmentContext().getSegmentPath().toFile()))) {
            while (databaseInputStream.available() > 0) {
                long currentStreamPosition = databaseInputStream.getReadBytes();
                Optional<DatabaseRecord> optionalDatabaseRecord = databaseInputStream.readDbUnit();
                String lastKey = new String(databaseInputStream.getLastKeyObject());

                optionalDatabaseRecord.ifPresentOrElse(
                        (databaseRecord) -> presentKeys.put(lastKey, new SegmentOffsetInfoImpl(currentStreamPosition)),
                        () -> presentKeys.remove(lastKey)
                );

            }
        } catch (IOException e) {
            throw new DatabaseException("Cannot create FileInputStream", e);
        }

        SegmentIndex segmentIndex = new SegmentIndex();
        for (Map.Entry<String, SegmentOffsetInfo> entry: presentKeys.entrySet()) {
            segmentIndex.onIndexedEntityUpdated(entry.getKey(), entry.getValue());
        }

        Segment segment = SegmentImpl.initializeFromContext(new SegmentInitializationContextImpl(
                context.currentSegmentContext().getSegmentName(),
                context.currentSegmentContext().getSegmentPath(),
                context.currentSegmentContext().getCurrentSize(),
                segmentIndex
        ));

        context.currentTableContext().updateCurrentSegment(segment);

        for (String key: presentKeys.keySet()) {
            context.currentTableContext().getTableIndex().onIndexedEntityUpdated(key, segment);
        }
    }
}
