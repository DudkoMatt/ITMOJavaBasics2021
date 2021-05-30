package com.itmo.java.basics.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;

/**
 * Класс, отвечающий за подгрузку данных из конфигурационного файла формата .properties
 */
public class ConfigLoader {
    private final static String DEFAULT_PROPERTY_FILENAME = "src/main/resources/server.properties";
    private final static String HOST_KEY_DICTIONARY =  "kvs.host";
    private final static String PORT_KEY_DICTIONARY = "kvs.port";
    private final static String WORKING_PATH_KEY_DICTIONARY = "kvs.workingPath";

    private final String propertiesFilename;
    private final HashMap<String, String> propertiesDictionary;

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
        propertiesFilename = name;
        propertiesDictionary = new HashMap<>();

    }

    /**
     * Считывает конфиг из указанного в конструкторе файла.
     * Если не удалось считать из заданного файла, или какого-то конкретно значения не оказалось,
     * то используют дефолтные значения из {@link DatabaseConfig} и {@link ServerConfig}
     * <br/>
     * Читаются: "kvs.workingPath", "kvs.host", "kvs.port" (но в конфигурационном файле допустимы и другие проперти)
     */
    public DatabaseServerConfig readConfig() {
        loadAllProperties();

        return DatabaseServerConfig.builder()
                .serverConfig(new ServerConfig(propertiesDictionary.get(HOST_KEY_DICTIONARY), Integer.parseInt(propertiesDictionary.get(PORT_KEY_DICTIONARY))))
                .dbConfig(new DatabaseConfig(propertiesDictionary.get(WORKING_PATH_KEY_DICTIONARY)))
                .build();
    }

    private void loadAllProperties() {
        try (Scanner scanner = new Scanner(new FileInputStream(propertiesFilename))) {
            while (scanner.hasNext()) {
                String[] propertyLine = scanner.nextLine().split("=", 2);
                propertiesDictionary.put(propertyLine[0], propertyLine[1]);
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(String.format("File %s does not exist", propertiesFilename), e);
        }
    }
}
