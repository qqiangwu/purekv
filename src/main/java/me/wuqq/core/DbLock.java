package me.wuqq.core;

public interface DbLock {
    boolean tryLock();
    void unlock();
    boolean isLocked();
}
