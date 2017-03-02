package me.wuqq.core;

import lombok.Value;

import java.util.Iterator;
import java.util.Optional;

public interface MemoryTable {
    @Value
    class Record {
        long sequenceNumber;
        boolean present;
        byte[] key;
        byte[] value;
    }

    long sizeInBytes();

    void write(long sequenceNumber, byte[] key, byte[] value);
    void remove(long sequenceNumber, byte[] key);

    Optional<Record> get(byte[] key);

    Iterator<Record> iterator();
}
