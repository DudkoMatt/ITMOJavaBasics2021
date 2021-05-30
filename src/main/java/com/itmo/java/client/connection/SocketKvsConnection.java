package com.itmo.java.client.connection;

import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.net.Socket;

/**
 * С помощью {@link RespWriter} и {@link RespReader} читает/пишет в сокет
 */
public class SocketKvsConnection implements KvsConnection {
    private final Socket socket;
    private final RespReader reader;
    private final RespWriter writer;

    public SocketKvsConnection(ConnectionConfig config) {
        try {
            this.socket = new Socket(config.getHost(), config.getPort());
            this.reader = new RespReader(socket.getInputStream());
            this.writer = new RespWriter(socket.getOutputStream());
        } catch (IOException e) {
            throw new RuntimeException("Cannot create SocketKvsConnection", e);
        }
    }

    /**
     * Отправляет с помощью сокета команду и получает результат.
     * @param commandId id команды (номер)
     * @param command   команда
     * @throws ConnectionException если сокет закрыт или если произошла другая ошибка соединения
     */
    @Override
    public synchronized RespObject send(int commandId, RespArray command) throws ConnectionException {
        try {
            writer.write(command);
            while (!reader.hasAvailableData()) {

            }
            return reader.readObject();
        } catch (IOException e) {
            throw new ConnectionException(String.format("Command sending error. CommandID: %d", commandId), e);
        }
    }

    /**
     * Закрывает сокет (и другие использованные ресурсы)
     */
    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException ignore) {
            // Ignore errors on closing
        }

        try {
            reader.close();
        } catch (IOException ignore) {
            // Ignore errors on closing
        }

        try {
            writer.close();
        } catch (IOException ignore) {
            // Ignore errors on closing
        }
    }
}
