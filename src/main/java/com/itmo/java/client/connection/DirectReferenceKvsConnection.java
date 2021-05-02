package com.itmo.java.client.connection;

import com.itmo.java.basics.DatabaseServer;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.util.concurrent.ExecutionException;

/**
 * Реализация подключения, когда есть прямая ссылка на объект
 * (пока еще нет реализации сокетов)
 */
public class DirectReferenceKvsConnection implements KvsConnection {
    private final DatabaseServer databaseServer;

    public DirectReferenceKvsConnection(DatabaseServer databaseServer) {
        this.databaseServer = databaseServer;
    }

    @Override
    public RespObject send(int commandId, RespArray command) throws ConnectionException {  // ToDO: нужно еще DatabaseExecutionException
        // ToDO: зачем нам счетчик команд

        try {
            // ToDO: точно ли кастим???
            return databaseServer.executeNextCommand(command).get().serialize();
        } catch (ExecutionException | InterruptedException e) {
            throw new ConnectionException("Connection error", e);  // ToDO: DatabaseExecutionException
        }
    }

    /**
     * Ничего не делает ¯\_(ツ)_/¯
     */
    @Override
    public void close() {
    }
}
