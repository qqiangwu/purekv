package me.wuqq.memtable;

import me.wuqq.core.MemoryTable;

public interface PersistentMemoryTable extends MemoryTable {
    /** Destroy the persistent storage */
    void destroy();
}
