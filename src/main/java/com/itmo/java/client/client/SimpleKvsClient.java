package com.itmo.java.client.client;

import com.itmo.java.client.command.*;
import com.itmo.java.client.connection.KvsConnection;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.client.exception.DatabaseExecutionException;
import com.itmo.java.protocol.model.RespObject;

import java.util.function.Supplier;

public class SimpleKvsClient implements KvsClient {
    private final String databaseName;
    private final KvsConnection connection;

    /**
     * Конструктор
     *
     * @param databaseName       имя базы, с которой работает
     * @param connectionSupplier метод создания подключения к базе
     */
    public SimpleKvsClient(String databaseName, Supplier<KvsConnection> connectionSupplier) {
        this.databaseName = databaseName;
        this.connection = connectionSupplier.get();
    }

    @Override
    public String createDatabase() throws DatabaseExecutionException {
        try {
            return sendCommand(new CreateDatabaseKvsCommand(databaseName));
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("Cannot create database with name %s", databaseName), e);
        }
    }

    @Override
    public String createTable(String tableName) throws DatabaseExecutionException {
        try {
            return sendCommand(new CreateTableKvsCommand(databaseName, tableName));
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("Cannot create table %s in database %s", tableName, databaseName), e);
        }
    }

    @Override
    public String get(String tableName, String key) throws DatabaseExecutionException {
        try {
            return sendCommand(new GetKvsCommand(databaseName, tableName, key));
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("Cannot get key %s from table %s in database %s", key, tableName, databaseName), e);
        }
    }

    @Override
    public String set(String tableName, String key, String value) throws DatabaseExecutionException {
        try {
            return sendCommand(new SetKvsCommand(databaseName, tableName, key, value));
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("Cannot set value %s to key %s from table %s in database %s", value, key, tableName, databaseName), e);
        }
    }

    @Override
    public String delete(String tableName, String key) throws DatabaseExecutionException {
        try {
            return sendCommand(new DeleteKvsCommand(databaseName, tableName, key));
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("Cannot delete value from key %s from table %s in database %s", key, tableName, databaseName), e);
        }
    }

    private String sendCommand(KvsCommand command) throws ConnectionException, DatabaseExecutionException {
        RespObject object = connection.send(command.getCommandId(), command.serialize());

        if (object.isError()) {
            throw new DatabaseExecutionException(object.asString());
        }

        return object.asString();
    }
}
