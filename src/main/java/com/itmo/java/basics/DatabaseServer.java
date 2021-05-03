package com.itmo.java.basics;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.DatabaseCommands;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.InitializationContextImpl;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseServer {
    private final ExecutionEnvironment env;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    /**
     * Конструктор
     *
     * @param env         env для инициализации. Далее работа происходит с заполненным объектом
     * @param initializer готовый чейн инициализации
     * @throws DatabaseException если произошла ошибка инициализации
     */
    public static DatabaseServer initialize(ExecutionEnvironment env, DatabaseServerInitializer initializer) throws DatabaseException {
        initializer.perform(InitializationContextImpl.builder().executionEnvironment(env).build());
        return new DatabaseServer(env);
    }

    private DatabaseServer(ExecutionEnvironment env) {
        this.env = env;
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(RespArray message) {
        // ToDO: зачем нам счетчик команд

        List<RespObject> objects = message.getObjects();
        RespObject commandName = objects.get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex());
        return executeNextCommand(DatabaseCommands.valueOf(commandName.asString()).getCommand(env, objects));
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(DatabaseCommand command) {
        return CompletableFuture.supplyAsync(command::execute, executorService);
    }
}