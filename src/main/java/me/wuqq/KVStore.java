package me.wuqq;

public interface KVStore extends AutoCloseable {
    byte[] read(byte[] key);
    void write(byte[] key, byte[] value);
    void delete(byte[] key);
}