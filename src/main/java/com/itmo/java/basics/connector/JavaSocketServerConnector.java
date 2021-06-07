package com.itmo.java.basics.connector;

import com.itmo.java.basics.DatabaseServer;
import com.itmo.java.basics.config.ConfigLoader;
import com.itmo.java.basics.config.DatabaseServerConfig;
import com.itmo.java.basics.config.ServerConfig;
import com.itmo.java.basics.console.impl.ExecutionEnvironmentImpl;
import com.itmo.java.basics.initialization.impl.DatabaseInitializer;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.SegmentInitializer;
import com.itmo.java.basics.initialization.impl.TableInitializer;
import com.itmo.java.basics.resp.CommandReader;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Класс, который предоставляет доступ к серверу через сокеты
 */
public class JavaSocketServerConnector implements Closeable {

    /**
     * Экзекьютор для выполнения ClientTask
     */
    private final ExecutorService clientIOWorkers = Executors.newSingleThreadExecutor();

    private final ServerSocket serverSocket;
    private final ExecutorService connectionAcceptorExecutor = Executors.newSingleThreadExecutor();
    private final DatabaseServer databaseServer;

    /**
     * Стартует сервер. По аналогии с сокетом открывает коннекшн в конструкторе.
     */
    public JavaSocketServerConnector(DatabaseServer databaseServer, ServerConfig config) throws IOException {
        serverSocket = new ServerSocket(config.getPort());
        this.databaseServer = databaseServer;
    }
 
     /**
     * Начинает слушать заданный порт, начинает аксептить клиентские сокеты. На каждый из них начинает клиентскую таску
     */
    public void start() {
        connectionAcceptorExecutor.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Socket client = serverSocket.accept();
                    new ClientTask(client, databaseServer).run();
                } catch (IOException e) {
                    throw new RuntimeException("Cannot accept client and start new thread", e);
                }
            }
        });
    }

    /**
     * Закрывает все, что нужно ¯\_(ツ)_/¯
     */
    @Override
    public void close() {
        System.out.println("Stopping socket connector");
        clientIOWorkers.shutdown();
        connectionAcceptorExecutor.shutdown();

        try {
            serverSocket.close();
        } catch (IOException ignore) {
            // Ignore errors on closing
        }
    }


    public static void main(String[] args) throws Exception {
        // Читаем настройки
        DatabaseServerConfig databaseServerConfig = new ConfigLoader().readConfig();

        // Создаем DatabaseServer
        var database = DatabaseServer.initialize(new ExecutionEnvironmentImpl(databaseServerConfig.getDbConfig()),
                new DatabaseServerInitializer(new DatabaseInitializer(new TableInitializer(new SegmentInitializer()))));

        // Создаем JavaSocketServerConnector
        JavaSocketServerConnector javaSocketServerConnector = new JavaSocketServerConnector(database, databaseServerConfig.getServerConfig());

        // Запускаем сервер
        javaSocketServerConnector.start();
    }

    /**
     * Runnable, описывающий исполнение клиентской команды.
     */
    static class ClientTask implements Runnable, Closeable {
        private final Socket client;
        private final DatabaseServer server;
        private final RespReader reader;
        private final RespWriter writer;

        /**
         * @param client клиентский сокет
         * @param server сервер, на котором исполняется задача
         */
        public ClientTask(Socket client, DatabaseServer server) {
            this.client = client;
            this.server = server;

            try {
                this.reader = new RespReader(client.getInputStream());
                this.writer = new RespWriter(client.getOutputStream());
            } catch (IOException e) {
                close();
                throw new RuntimeException("Cannot create RespReader/RespWriter for socket", e);
            }
        }

        /**
         * Исполняет задачи из одного клиентского сокета, пока клиент не отсоединился или текущий поток не был прерван (interrupted).
         * Для кажной из задач:
         * 1. Читает из сокета команду с помощью {@link CommandReader}
         * 2. Исполняет ее на сервере
         * 3. Записывает результат в сокет с помощью {@link RespWriter}
         */
        @Override
        public void run() {
            try (CommandReader commandReader = new CommandReader(reader, server.getEnv())) {
                while (client.isConnected() && !Thread.currentThread().isInterrupted()) {
                    try {
                        if (commandReader.hasNextCommand()) {
                            writer.write(server.executeNextCommand(commandReader.readCommand()).get().serialize());
                        }
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                close();
            }
        }

        /**
         * Закрывает клиентский сокет
         */
        @Override
        public void close() {
            try {
                client.close();
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
}
