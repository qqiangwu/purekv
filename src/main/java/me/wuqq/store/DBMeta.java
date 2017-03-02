package me.wuqq.store;

import lombok.val;
import me.wuqq.core.OperationLogAppender;
import me.wuqq.support.NotThreadSafe;

@NotThreadSafe
public class DBMeta {
    private long mSequenceNumberSeen;
    private long mFileNumberSeen;
    private final String mDbName;
    private final OperationLogAppender mMetaLog;

    public DBMeta(final long sequenceNumberSeen,
                  final long fileNumberSeen,
                  final String dbName,
                  final OperationLogAppender metaLog) {
        this.mSequenceNumberSeen = sequenceNumberSeen;
        this.mFileNumberSeen = fileNumberSeen;
        this.mDbName = dbName;
        this.mMetaLog = metaLog;
    }

    public long nextSequenceNumber() {
        return ++mSequenceNumberSeen;
    }

    public long nextFileNumber() {
        return ++mFileNumberSeen;
    }

    public long currentSequenceNumber() {
        return mSequenceNumberSeen;
    }

    public long currentFileNumber() {
        return mFileNumberSeen;
    }

    public void advanceFileNumber(final long fileNumberSeen) {
        mFileNumberSeen = Math.max(fileNumberSeen, mFileNumberSeen);
    }

    public void advanceSequenceNumber(final long sequenceNumberSeen) {
        mSequenceNumberSeen = Math.max(sequenceNumberSeen, mSequenceNumberSeen);
    }

    public String getDbName() {
        return mDbName;
    }

    public void logChanges(final MetaChangeCollector collector) {
        collector.setFileNumberSeen(mFileNumberSeen);
        collector.setSequenceNumberSeen(mSequenceNumberSeen);

        val record = MetaLogFormatter.encode(collector);

        mMetaLog.append(record);
        mMetaLog.sync();
    }
}
