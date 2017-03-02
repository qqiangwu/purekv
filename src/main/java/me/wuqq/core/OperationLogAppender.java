package me.wuqq.core;

public interface OperationLogAppender extends AutoCloseable {
    void append(byte[] frame);
    void sync();
    long size();
}
