package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
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
        HashSet<String> presentKeys = new HashSet<>();

        try (DatabaseInputStream databaseInputStream = new DatabaseInputStream(new FileInputStream(context.currentSegmentContext().getSegmentPath().toFile()))) {
            long currentPosition = 0;
            while (databaseInputStream.available() > 0) {
                Optional<DatabaseRecord> optionalDatabaseRecord = databaseInputStream.readDbUnit();

                if (optionalDatabaseRecord.isEmpty()) {
                    throw new DatabaseException("DatabaseInputStream should not return optional empty");
                }

                DatabaseRecord databaseRecord = optionalDatabaseRecord.get();
                String lastKey = new String(databaseRecord.getKey());
                context.currentSegmentContext().getIndex().onIndexedEntityUpdated(lastKey, new SegmentOffsetInfoImpl(currentPosition));
                presentKeys.add(lastKey);

                // ToDO: no need to add removed keys..?
//                optionalDatabaseRecord.ifPresentOrElse(
//                        (databaseRecord) -> presentKeys.add(lastKey),
//                        () -> presentKeys.remove(lastKey)
//                );

                currentPosition += databaseRecord.size();
            }
        } catch (IOException e) {
            throw new DatabaseException("Cannot create FileInputStream", e);
        }

        Segment segment = SegmentImpl.initializeFromContext(context.currentSegmentContext());

        context.currentTableContext().updateCurrentSegment(segment);

        for (String key: presentKeys) {
            context.currentTableContext().getTableIndex().onIndexedEntityUpdated(key, segment);
        }
    }
}
