package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class DatabaseInitializer implements Initializer {
    private final TableInitializer tableInitializer;

    public DatabaseInitializer(TableInitializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой бд.
     * Запускает инициализацию всех таблиц это базы
     *
     * @param initialContext контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к базе, невозможно прочитать содержимого папки,
     *  или если возникла ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext initialContext) throws DatabaseException {
        Path workingPath = initialContext.currentDbContext().getDatabasePath();

        try {
            for (Path tableDirectory : Files.newDirectoryStream(workingPath)) {
                if (Files.isDirectory(tableDirectory)) {
                    tableInitializer.perform(
                            InitializationContextImpl.builder()
                                    .executionEnvironment(initialContext.executionEnvironment())
                                    .currentDatabaseContext(initialContext.currentDbContext())
                                    .currentTableContext(new TableInitializationContextImpl(new File(tableDirectory.toString()).getName(), workingPath, new TableIndex()))
                                    .build()
                    );
                }
            }

            initialContext.executionEnvironment().addDatabase(DatabaseImpl.initializeFromContext(initialContext.currentDbContext()));
        } catch (IOException e) {
            throw new DatabaseException("Cannot iterate over directory", e);
        }
    }
}
