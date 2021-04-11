package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class DatabaseServerInitializer implements Initializer {
    private final DatabaseInitializer databaseInitializer;

    public DatabaseServerInitializer(DatabaseInitializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }

    /**
     * Если заданная в окружении директория не существует - создает ее
     * Добавляет информацию о существующих в директории базах, нацинает их инициалиализацию
     *
     * @param context контекст, содержащий информацию об окружении
     * @throws DatabaseException если произошла ошибка при создании директории, ее обходе или ошибка инициализации бд
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        Path workingPath = context.executionEnvironment().getWorkingPath();
        if (!Files.exists(workingPath)) {
            try {
                Files.createDirectory(workingPath);
            } catch (IOException e) {
                throw new DatabaseException("Cannot create a directory for working path", e);
            }
        }

        if (!Files.isDirectory(workingPath)) {
            throw new DatabaseException("Working path is a file, not directory");
        }

        try {
            for (Path db_directory : Files.newDirectoryStream(workingPath)) {
                databaseInitializer.perform(
                        InitializationContextImpl.builder()
                                .executionEnvironment(context.executionEnvironment())
                                .currentDatabaseContext(
                                        new DatabaseInitializationContextImpl(
                                            new File(db_directory.toString()).getName(), workingPath
                                        )
                                )
                                .build()
                );
            }
        } catch (IOException e) {
            throw new DatabaseException("Cannot iterate over directory", e);
        }
    }
}
