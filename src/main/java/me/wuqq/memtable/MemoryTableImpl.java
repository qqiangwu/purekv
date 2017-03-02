package me.wuqq.memtable;

import lombok.val;
import me.wuqq.core.MemoryTable;
import me.wuqq.support.NotThreadSafe;

import java.util.*;

@NotThreadSafe
public class MemoryTableImpl implements MemoryTable {
    private static byte[] ABSENT_VALUE = new byte[0];
    private long mBytes = 0;
    private Map<byte[], Record> mEntries = new HashMap<>();

    @Override
    public long sizeInBytes() {
        return mBytes;
    }

    @Override
    public void write(final long sequenceNumber, final byte[] key, final byte[] value) {
        val estimatedBytes = estimateBytes(key, value);
        val immutableKey = Arrays.copyOf(key, key.length);
        val immutableVal = Arrays.copyOf(value, value.length);
        val e = new Record(sequenceNumber, true, immutableKey, immutableVal);

        mEntries.put(immutableKey, e);
        mBytes += estimatedBytes;
    }

    private long estimateBytes(final byte[] key, final byte[] value) {
        return key.length + value.length;
    }

    @Override
    public void remove(final long sequenceNumber, final byte[] key) {
        val estimatedBytes = estimateBytes(key, ABSENT_VALUE);
        val immutableKey = Arrays.copyOf(key, key.length);

        mEntries.put(immutableKey, new Record(sequenceNumber, false, immutableKey, ABSENT_VALUE));
        mBytes += estimatedBytes;
    }

    @Override
    public Optional<Record> get(final byte[] key) {
        return Optional.ofNullable(mEntries.get(key));
    }

    @Override
    public Iterator<Record> iterator() {
        return mEntries.values().iterator();
    }
}
