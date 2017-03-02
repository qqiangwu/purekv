package me.wuqq.support;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public abstract class DbFilenameGenerator {
    public static final Path getLogDir(final String dbName) {
        return Paths.get(dbName, "logs");
    }

    public static final Path getDataDir(final String dbName) {
        return Paths.get(dbName, "data");
    }

    public static final Path getLogFilename(final String dbName, final long fileNumber) {
        return getLogDir(dbName).resolve(Long.toString(fileNumber));
    }

    public static final Path getDataFilename(final String dbName, final long fileNumber) {
        return getDataDir(dbName).resolve(Long.toString(fileNumber));
    }

    public static final Path getMetaFilename(final String dbName, final long fileNumber) {
        return Paths.get(dbName, Long.toString(fileNumber));
    }

    public static final Path getCurrentFilename(final String dbName) {
        return Paths.get(dbName, ".current");
    }
    
    public static final Path getLockFilename(final String dbName) {
        return Paths.get(dbName, ".lock");
    }
}
