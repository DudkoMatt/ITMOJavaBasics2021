package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
public class DatabaseImpl implements Database {
    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        if (new File(databaseRoot.toString(), dbName).exists()) {
            throw new DatabaseException("Database already exists");
        }

        return new DatabaseImpl(dbName, databaseRoot);
    }

    private final String dbName;
    private final Path databaseRootPath;
    private final Map<String, Table> tables;

    private DatabaseImpl(String dbName, Path databaseRoot) throws DatabaseException {
        this.dbName = dbName;
        this.databaseRootPath = Paths.get(databaseRoot.toString(), dbName);
        this.tables = new HashMap<>();

        try {
            Files.createDirectory(databaseRootPath);
        } catch (IOException e) {
            throw new DatabaseException("Cannot create directory for a database", e);
        }
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        return DatabaseImpl.builder()
                .dbName(context.getDbName())
                .databaseRootPath(context.getDatabasePath())
                .tables(context.getTables())
                .build();
    }

    @Override
    public String getName() {
        return this.dbName;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (tables.containsKey(tableName)) {
            throw new DatabaseException("Table already exists");
        }

        tables.put(tableName, TableImpl.create(tableName, databaseRootPath, new TableIndex()));
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        checkIfTableExists(tableName);
        if (objectValue == null) {
            tables.get(tableName).delete(objectKey);
        } else {
            tables.get(tableName).write(objectKey, objectValue);
        }
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        checkIfTableExists(tableName);
        return tables.get(tableName).read(objectKey);
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        checkIfTableExists(tableName);
        tables.get(tableName).delete(objectKey);
    }

    private void checkIfTableExists(String tableName) throws DatabaseException {
        if (!tables.containsKey(tableName)) {
            throw new DatabaseException("Table with name " + tableName + " was not found");
        }
    }
}
