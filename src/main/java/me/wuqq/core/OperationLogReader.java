package me.wuqq.core;

import java.util.Optional;

public interface OperationLogReader extends AutoCloseable {
    class LogCorruptionException extends RuntimeException {
        public LogCorruptionException(String message) {
            super(message);
        }
    }

    Optional<byte[]> read();
}
