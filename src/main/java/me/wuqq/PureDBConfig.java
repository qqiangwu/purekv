package me.wuqq;

import me.wuqq.core.Env;
import me.wuqq.support.EnvImpl;

public class PureDBConfig {
    private final String mDbName;
    private boolean mCreateIfMissing = false;
    private Env mEnv = EnvImpl.getInstance();

    public PureDBConfig(final String dbName) {
        mDbName = dbName;
    }

    public void createIfMissing() {
        mCreateIfMissing = true;
    }

    public void setEnv(final Env env) {
        mEnv = env;
    }

    public String getDbName() {
        return mDbName;
    }

    public boolean needCreateIfMissing() {
        return mCreateIfMissing;
    }

    public Env getEnv() {
        return mEnv;
    }
}
