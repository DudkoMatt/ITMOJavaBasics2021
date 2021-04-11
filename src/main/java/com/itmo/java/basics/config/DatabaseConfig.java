package com.itmo.java.basics.config;

public class DatabaseConfig {
    public static final String DEFAULT_WORKING_PATH = "db_files";
    private final String workingPath;

    public DatabaseConfig(String workingPath) {
        this.workingPath = workingPath;
    }

    public String getWorkingPath() {
        return workingPath == null ? DEFAULT_WORKING_PATH : workingPath;
    }
}
