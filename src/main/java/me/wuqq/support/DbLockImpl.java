package me.wuqq.support;

import lombok.SneakyThrows;
import me.wuqq.core.DbLock;
import org.jetbrains.annotations.Nullable;

import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public class DbLockImpl implements DbLock {
    private final FileChannel mUnderlyingFile;

    @Nullable
    private FileLock mFileLock = null;

    public DbLockImpl(final FileChannel channel) {
        mUnderlyingFile = channel;
    }

    @Override
    @SneakyThrows
    public boolean tryLock() {
        if (isLocked()) {
            throw new IllegalStateException("Already locked");
        }

        mFileLock = mUnderlyingFile.tryLock();

        return mFileLock != null && mFileLock.isValid();
    }

    @Override
    @SneakyThrows
    public void unlock() {
        if (!isLocked()) {
            throw new IllegalStateException("Not locked");
        }

        mFileLock.release();
        mFileLock = null;
    }

    @Override
    public boolean isLocked() {
        return mFileLock != null && mFileLock.isValid();
    }
}
