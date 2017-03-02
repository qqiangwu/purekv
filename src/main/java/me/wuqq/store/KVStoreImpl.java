package me.wuqq.store;

import me.wuqq.KVStore;
import me.wuqq.core.DbLock;
import me.wuqq.core.MemoryTable;
import me.wuqq.core.SortedTableManager;
import me.wuqq.support.ThreadSafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@ThreadSafe
public class KVStoreImpl implements KVStore {
    @NotNull
    private final DbLock mDbLock;

    @NotNull
    private MemoryTable mMemTable;

    @Nullable
    private MemoryTable mImmutableTable;

    @NotNull
    private final SortedTableManager mTableManger;

    @NotNull
    private final DBMeta mMeta;

    public KVStoreImpl(DbLock dbLock,
                       DBMeta meta,
                       SortedTableManager tableManger) {
        this.mDbLock = dbLock;
        this.mMemTable = null; // fixme
        this.mImmutableTable = null;
        this.mTableManger = tableManger;
        this.mMeta = meta;
    }

    @Override
    public byte[] read(byte[] key) {
        return new byte[0];
    }

    @Override
    public void write(byte[] key, byte[] value) {

    }

    @Override
    public void delete(byte[] key) {

    }

    @Override
    public void close() throws Exception {

    }

    private void doMinorCompact() {

    }

    private void doMajorCompcat() {

    }
}
