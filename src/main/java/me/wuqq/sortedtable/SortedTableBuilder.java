package me.wuqq.sortedtable;

import me.wuqq.core.MemoryTable;
import me.wuqq.core.SortedFileMeta;

import java.nio.channels.WritableByteChannel;

public class SortedTableBuilder {
    private WritableByteChannel mOutput;

    public SortedTableBuilder(long dataFileID, final WritableByteChannel output) {
        this.mOutput = output;
    }

    public SortedFileMeta finish() {
        return null;
    }

    public void add(long sequenceNumber, byte[] key, byte[] value) {
    }

    public void add(MemoryTable.Record record) {

    }
}
