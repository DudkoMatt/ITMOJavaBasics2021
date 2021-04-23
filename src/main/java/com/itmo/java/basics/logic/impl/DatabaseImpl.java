package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DatabaseImpl implements Database {
    private final String dbName;
    private final Path databaseRootPath;
    private final Map<String, Table> tables;

    private DatabaseImpl(String dbName, Path databaseRoot) {
        this(dbName, databaseRoot, new HashMap<>());
    }

    private DatabaseImpl(String dbName, Path databaseRoot, Map<String, Table> tables) {
        this.dbName = dbName;
        this.databaseRootPath = Paths.get(databaseRoot.toString(), dbName);
        this.tables = tables;
    }

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        if (new File(databaseRoot.toString(), dbName).exists()) {
            throw new DatabaseException("Database already exists");
        }

        try {
            Files.createDirectory(Paths.get(databaseRoot.toString(), dbName));
        } catch (IOException e) {
            throw new DatabaseException("Cannot create directory for a database", e);
        }

        return new DatabaseImpl(dbName, databaseRoot);
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        return new DatabaseImpl(context.getDbName(), context.getDatabasePath(), context.getTables());
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
