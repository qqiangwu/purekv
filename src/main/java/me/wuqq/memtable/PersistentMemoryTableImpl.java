package me.wuqq.memtable;

import lombok.SneakyThrows;
import lombok.val;
import me.wuqq.core.MemoryTable;
import me.wuqq.core.OperationLogAppender;
import me.wuqq.log.OperationLogAppenderImpl;
import me.wuqq.support.NotThreadSafe;

import java.io.File;
import java.io.FileInputStream;
import java.util.Iterator;
import java.util.Optional;

@NotThreadSafe
public class PersistentMemoryTableImpl implements PersistentMemoryTable {
    private final MemoryTable mTable = new MemoryTableImpl();
    private final OperationLogAppender mLogAppender;
    private final File mLogFile;
    private final MemoryTableLogFormatter mRecordFormatter = new MemoryTableLogFormatter();

    @SneakyThrows
    public PersistentMemoryTableImpl(final File logFile) {
        logFile.createNewFile();

        mLogFile = logFile;
        mLogAppender = new OperationLogAppenderImpl(new FileInputStream(logFile).getChannel());
    }

    @Override
    @SneakyThrows
    public void destroy() {
        mLogAppender.sync();
        mLogAppender.close();
        mLogFile.delete();
    }

    @Override
    public long sizeInBytes() {
        return mTable.sizeInBytes();
    }

    @Override
    public void write(final long sequenceNumber, final byte[] key, final byte[] value) {
        appendLog(sequenceNumber, key, value);

        mTable.write(sequenceNumber, key, value);
    }

    @Override
    public void remove(final long sequenceNumber, final byte[] key) {
        appendLog(sequenceNumber, key, null);

        mTable.remove(sequenceNumber, key);
    }

    @Override
    public Optional<Record> get(final byte[] key) {
        return mTable.get(key);
    }

    @Override
    public Iterator<Record> iterator() {
        return mTable.iterator();
    }

    private void appendLog(final long sequenceNumber, final byte[] key, final byte[] value) {
        val record = mRecordFormatter.encodeRecord(sequenceNumber, key, value);
        mLogAppender.append(record);
        mLogAppender.sync();
    }
}
