package com.itmo.java.basics.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Класс, отвечающий за подгрузку данных из конфигурационного файла формата .properties
 */
public class ConfigLoader {
    private final static String DEFAULT_PROPERTY_FILENAME = "server.properties";
    private final static String HOST_PROPERTY =  "kvs.host";
    private final static String PORT_PROPERTY = "kvs.port";
    private final static String WORKING_PATH_PROPERTY = "kvs.workingPath";

    private final Properties properties;

    /**
     * По умолчанию читает из server.properties
     */
    public ConfigLoader() {
        this(DEFAULT_PROPERTY_FILENAME);
    }

    /**
     * @param name Имя конфикурационного файла, откуда читать
     */
    public ConfigLoader(String name) {
        properties = new Properties();

        try {
            InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(name);
            if (inputStream == null) {
                inputStream = new FileInputStream(name);
            }

            properties.load(inputStream);
        } catch (IOException ignore) {
            // Ignore and use defaults
        }
    }

    /**
     * Считывает конфиг из указанного в конструкторе файла.
     * Если не удалось считать из заданного файла, или какого-то конкретно значения не оказалось,
     * то используют дефолтные значения из {@link DatabaseConfig} и {@link ServerConfig}
     * <br/>
     * Читаются: "kvs.workingPath", "kvs.host", "kvs.port" (но в конфигурационном файле допустимы и другие проперти)
     */
    public DatabaseServerConfig readConfig() {
        String host = properties.getProperty(HOST_PROPERTY, ServerConfig.DEFAULT_HOST);
        int port = Integer.parseInt(properties.getProperty(PORT_PROPERTY, String.valueOf(ServerConfig.DEFAULT_PORT)));
        String workingPath = properties.getProperty(WORKING_PATH_PROPERTY, DatabaseConfig.DEFAULT_WORKING_PATH);

        return DatabaseServerConfig.builder()
                .serverConfig(new ServerConfig(host, port))
                .dbConfig(new DatabaseConfig(workingPath))
                .build();
    }
}
