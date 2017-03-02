package me.wuqq.store;

import me.wuqq.core.FileLevel;

import java.util.HashMap;
import java.util.Map;

public class MetaChangeCollector {

    private long mSequenceNumberSeen = -1;
    private long mFileNumberSeen = -1;

    private final Map<FileLevel, Long> mFileRemoved = new HashMap<>();
    private final Map<FileLevel, Long> mFileAdded = new HashMap<>();


    public void addDataFiles(final FileLevel level, final long fileAdded) {
        mFileAdded.put(level, fileAdded);
    }

    public void removeDataFiles(final FileLevel level, final long fileRemoved) {
        mFileRemoved.put(level, fileRemoved);
    }

    public void setSequenceNumberSeen(final long sequenceNumberSeen) {
        mSequenceNumberSeen = sequenceNumberSeen;
    }

    public void setFileNumberSeen(final long fileNumberSeen) {
        mFileNumberSeen = fileNumberSeen;
    }

    public long getSequenceNumberSeen() {
        return mSequenceNumberSeen;
    }

    public long getFileNumberSeen() {
        return mFileNumberSeen;
    }

    public Iterable<Map.Entry<FileLevel, Long>> getFilesAdded() {
        return mFileAdded.entrySet();
    }

    public Iterable<Map.Entry<FileLevel, Long>> getFilesRemoved() {
        return mFileRemoved.entrySet();
    }
}
