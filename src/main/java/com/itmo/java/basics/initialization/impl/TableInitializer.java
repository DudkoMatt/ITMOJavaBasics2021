package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;

public class TableInitializer implements Initializer {
    private final SegmentInitializer segmentInitializer;

    public TableInitializer(SegmentInitializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой таблице.
     * Запускает инициализацию всех сегментов в порядке их создания (из имени)
     *
     * @param context контекст с информацией об инициализируемой бд, окружении, таблицы
     * @throws DatabaseException если в контексте лежит неправильный путь к таблице, невозможно прочитать содержимого папки,
     *  или если возникла ошибка ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        Path workingPath = context.currentTableContext().getTablePath();

        File[] files = new File(workingPath.toString()).listFiles();
        if (files == null) {
            throw new DatabaseException("Cannot get files from directory");
        }

        Arrays.sort(files, (o1, o2) -> {
            String[] o1_parts = o1.getName().split("_");
            String[] o2_parts = o2.getName().split("_");

            long diff = Long.parseLong(o1_parts[o1_parts.length - 1]) - Long.parseLong(o2_parts[o1_parts.length - 1]);

            return diff < 0 ? -1 : diff == 0 ? 0 : 1;
        });

        for (File segmentFile : files) {
            if (segmentFile.isFile()) {
                segmentInitializer.perform(
                        InitializationContextImpl.builder()
                                .executionEnvironment(context.executionEnvironment())
                                .currentDatabaseContext(context.currentDbContext())
                                .currentTableContext(context.currentTableContext())
                                .currentSegmentContext(new SegmentInitializationContextImpl(segmentFile.getName(), workingPath, segmentFile.length()))
                                .build()
                );
            }
        }

        context.currentDbContext().addTable(TableImpl.initializeFromContext(context.currentTableContext()));
    }
}
