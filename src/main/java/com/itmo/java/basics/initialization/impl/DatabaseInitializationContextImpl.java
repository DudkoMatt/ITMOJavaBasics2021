package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Table;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class DatabaseInitializationContextImpl implements DatabaseInitializationContext {
    private final String dbName;
    private final Path databaseRoot;
    private final Map<String, Table> tables;

    public DatabaseInitializationContextImpl(String dbName, Path databaseRoot) {
        this.dbName = dbName;
        this.databaseRoot = databaseRoot;
        tables = new HashMap<>();
    }

    @Override
    public String getDbName() {
        return dbName;
    }

    @Override
    public Path getDatabasePath() {
        return Paths.get(databaseRoot.toString(), dbName);
    }

    @Override
    public Map<String, Table> getTables() {
        return tables;
    }

    @Override
    public void addTable(Table table) {
        tables.put(table.getName(), table);
    }
}
