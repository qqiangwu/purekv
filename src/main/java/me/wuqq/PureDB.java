package me.wuqq;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.val;
import me.wuqq.core.*;
import me.wuqq.log.OperationLogAppenderImpl;
import me.wuqq.log.OperationLogReaderImpl;
import me.wuqq.sortedtable.SortedTableBuilder;
import me.wuqq.sortedtable.SortedTableManagerImpl;
import me.wuqq.store.*;
import me.wuqq.support.DbFilenameGenerator;
import me.wuqq.support.DbLockImpl;
import me.wuqq.memtable.MemoryTableImpl;
import me.wuqq.memtable.MemoryTableLogFormatter;

import java.io.RandomAccessFile;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

// FIXME    all operations should be retrieved into their own class for clarity and simplicity
public abstract class PureDB {
    public class DbExistException extends RuntimeException {
        public DbExistException(final String dbName) {
            super(dbName + " already exists");
        }
    }

    public static final KVStore open(final PureDBConfig config) {
        return new DbOpener(config).open();
    }

    private static final class DbOpener {
        private final PureDBConfig mDbConfig;
        private final DbLock mDbLock;

        public DbOpener(final PureDBConfig config) {
            mDbConfig = config;
            mDbLock = acquireLock();
        }

        @SneakyThrows
        private DbLock acquireLock() {
            val lockFileName = DbFilenameGenerator.getLockFilename(mDbConfig.getDbName());
            val file = new RandomAccessFile(lockFileName.toString(), "rw").getChannel();
            val lock = new DbLockImpl(file);

            if (!lock.tryLock()) {
                throw new RuntimeException("Db already locked");
            }

            return lock;
        }

        public KVStore open() {
            try {
                return openExclusively();
            } catch (Exception e) {
                mDbLock.unlock();
                throw e;
            }
        }

        private KVStore openExclusively() {
            val metaCollector = loadMeta();
            val recovered = recover(metaCollector);

            snapshot(recovered);
            cleanup(recovered);

            return toDB(recovered);
        }

        @Value
        private static class RecoveredDb {
            DBMeta dbMeta;
            SortedTableManager sortedTableManager;
            MetaChangeCollector metaChangeCollector;
        }

        private MetaChangeCollector loadMeta() {
            val dbPointer = DbFilenameGenerator.getCurrentFilename(getDbName());

            if (getEnv().fileExists(dbPointer)) {
                return loadExistingMeta(dbPointer);
            } else if (mDbConfig.needCreateIfMissing()) {
                return loadNewMeta();
            } else {
                throw new RuntimeException(String.format("Database %s does not exist", getDbName()));
            }
        }

        private MetaChangeCollector loadNewMeta() {
            val metaCollector = new MetaChangeCollector();

            metaCollector.setSequenceNumberSeen(0);
            metaCollector.setFileNumberSeen(0);

            return metaCollector;
        }

        private MetaChangeCollector loadExistingMeta(final Path dbPointer) {
            @Cleanup
            val metaLogReader = openMetaLog(dbPointer);
            val collector = new MetaChangeCollector();

            replyMetaLog(metaLogReader, collector);

            return collector;
        }

        private OperationLogReader openMetaLog(final Path dbPointer) {
            val metaFileName = getEnv().readAsString(dbPointer);
            val metaLog = getEnv().newReadableChannel(Paths.get(metaFileName));
            return new OperationLogReaderImpl(metaLog);
        }

        private void replyMetaLog(final OperationLogReader metaLog, final MetaChangeCollector collector) {
            Optional<byte[]> buffer;

            while ((buffer = metaLog.read()).isPresent()) {
                val record = buffer.get();
                MetaLogFormatter.decode(record, collector);
            }
        }

        private RecoveredDb recover(final MetaChangeCollector metaCollector) {
            val dbMeta = recoverMeta(metaCollector);
            val sortedTableManager = recoverSortedTableManger(metaCollector);

            replyLogs(dbMeta, sortedTableManager, metaCollector);

            return new RecoveredDb(dbMeta, sortedTableManager, metaCollector);
        }

        private SortedTableManager recoverSortedTableManger(final MetaChangeCollector metaCollector) {
            return new SortedTableManagerImpl(metaCollector);
        }

        private DBMeta recoverMeta(final MetaChangeCollector metaCollector) {
            val nextFile = metaCollector.getFileNumberSeen() + 1;
            val metaLogName = DbFilenameGenerator.getLogFilename(getDbName(), nextFile);
            val metaLogChannel = getEnv().newWritableChannel(metaLogName);
            val metaLogAppender = new OperationLogAppenderImpl(metaLogChannel);
            return new DBMeta(metaCollector.getSequenceNumberSeen(), nextFile, getDbName(), metaLogAppender);
        }

        private void replyLogs(final DBMeta dbMeta,
                               final SortedTableManager sortedTableManager,
                               final MetaChangeCollector metaCollector) {
            val files = getLogFiles();

            files.stream().map(this::toFileID).forEach(fileId -> dbMeta.advanceFileNumber(fileId));
            files.forEach(log -> replyLog(log, dbMeta, sortedTableManager, metaCollector));
        }

        private long toFileID(final Path path) {
            return Long.parseLong(path.getFileName().toString());
        }

        private List<Path> getLogFiles() {
            val logDir = DbFilenameGenerator.getLogDir(getDbName());
            return getEnv().listDir(logDir);
        }

        private void replyLog(final Path log,
                              final DBMeta dbMeta,
                              final SortedTableManager sortedTableManager,
                              final MetaChangeCollector metaCollector) {
            val logFile = getEnv().newReadableChannel(log);
            @Cleanup
            val logReader = new OperationLogReaderImpl(logFile);

            MemoryTable memTable = new MemoryTableImpl();
            Optional<byte[]> rawRecord;

            while ((rawRecord = logReader.read()).isPresent()) {
                val record = MemoryTableLogFormatter.decodeRecord(rawRecord.get());

                applyRecord(memTable, record);
                dbMeta.advanceSequenceNumber(record.getSequenceNumber());

                if (isFull(memTable)) {
                    doMinorCompact(memTable, dbMeta, sortedTableManager);
                    memTable = new MemoryTableImpl();
                }
            }

            doMinorCompact(memTable, dbMeta, sortedTableManager);
        }

        private void doMinorCompact(final MemoryTable memTable,
                                    final DBMeta dbMeta,
                                    final SortedTableManager sortedTableManager) {
            val dataFileID = dbMeta.nextFileNumber();
            val dataFileName = DbFilenameGenerator.getDataFilename(getDbName(), dataFileID);
            @Cleanup
            val dataFile = getEnv().newWritableChannel(dataFileName);

            val fileMeta = writeTableToLevel0(dataFileID, memTable, dataFile);

            if (fileMeta.getFileSize() != 0) {
                sortedTableManager.addFile(FileLevel.LEVEL_0, fileMeta);
            }
        }

        private SortedFileMeta writeTableToLevel0(final long dataFileID,
                                                  final MemoryTable memTable,
                                                  final WritableByteChannel dataFile) {
            val tableBuilder = new SortedTableBuilder(dataFileID, dataFile);

            memTable.iterator().forEachRemaining(record -> tableBuilder.add(record));

            return tableBuilder.finish();
        }

        // FIXME
        private boolean isFull(final MemoryTable memTable) {
            return memTable.sizeInBytes() > 4 * 1024 * 1024;
        }

        private void applyRecord(final MemoryTable memTable, final MemoryTable.Record record) {
            if (record.isPresent()) {
                memTable.write(record.getSequenceNumber(), record.getKey(), record.getValue());
            } else {
                memTable.remove(record.getSequenceNumber(), record.getKey());
            }
        }

        // FIXME
        private void snapshot(final DbOpener.RecoveredDb recoveredDb) {
            val meta = recoveredDb.getDbMeta();
            val collector = recoveredDb.getMetaChangeCollector();

            meta.logChanges(collector);
        }

        private void cleanup(final RecoveredDb meta) {

        }

        private String getDbName() {
            return mDbConfig.getDbName();
        }

        private Env getEnv() {
            return mDbConfig.getEnv();
        }

        private KVStore toDB(final RecoveredDb recovered) {
            val meta = recovered.getDbMeta();
            val sortedTables = recovered.getSortedTableManager();

            return new KVStoreImpl(mDbLock, meta, sortedTables);
        }
    }
}
