package me.wuqq.store;

import lombok.SneakyThrows;
import lombok.val;
import me.wuqq.core.FileLevel;
import me.wuqq.support.NotThreadSafe;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Log format for manifest files
 *
 * record := change+
 *
 * change :=
 *      type : uint16
 *      data : binary
 *
 * if type == SEQUENCE_NUMBER_SEEN or FILE_NUMBER_SEEN,
 *      data = uint64 for the number
 * else:
 *      data :=
 *          level : uint8
 *          number : uint64 for the number
 */
@NotThreadSafe
public abstract class MetaLogFormatter {
    private enum ChangeType {
        SEQUENCE_NUMBER_SEEN,
        FILE_NUMBER_SEEN,
        ADD_FILE,
        REMOVE_FILE
    }

    private static final int TYPE_LEN = Short.BYTES;
    private static final int LEVEL_LEN = Short.BYTES;
    private static final int NUMBER_LEN = Long.BYTES;

    private static final ByteBuffer mBuffer = ByteBuffer.allocate(TYPE_LEN + LEVEL_LEN + NUMBER_LEN);
    private static final ByteArrayOutputStream mOutput = new ByteArrayOutputStream(mBuffer.capacity());

    public static final byte[] encode(final MetaChangeCollector changes) {
        prepareOutput();

        encodeNumberChange(ChangeType.SEQUENCE_NUMBER_SEEN, changes.getSequenceNumberSeen());
        encodeNumberChange(ChangeType.FILE_NUMBER_SEEN, changes.getFileNumberSeen());
        encodeFileChanges(ChangeType.ADD_FILE, changes.getFilesAdded());
        encodeFileChanges(ChangeType.REMOVE_FILE, changes.getFilesRemoved());

        return mOutput.toByteArray();
    }

    private static void prepareOutput() {
        mOutput.reset();
    }

    @SneakyThrows
    private static void encodeNumberChange(final ChangeType kind, final long number) {
        mBuffer.clear();

        mBuffer.putShort((short) kind.ordinal());
        mBuffer.putLong(number);

        mBuffer.flip();
        mOutput.write(mBuffer.array());
    }

    private static void encodeFileChanges(final ChangeType type,
                                          final Iterable<Map.Entry<FileLevel, Long>> changes) {
        for (val entry: changes) {
            encodeSingleFileChange(type, entry.getKey(), entry.getValue());
        }
    }

    @SneakyThrows
    private static void encodeSingleFileChange(final ChangeType type,
                                               final FileLevel level,
                                               final long file) {
        mBuffer.clear();

        mBuffer.putShort((short) type.ordinal());
        mBuffer.putShort((short) level.ordinal());
        mBuffer.putLong(file);

        mBuffer.flip();
        mOutput.write(mBuffer.array());
    }

    public static final void decode(final byte[] record, final MetaChangeCollector collector) {
        val buffer = ByteBuffer.wrap(record);

        while (buffer.hasRemaining()) {
            decodeOne(buffer, collector);
        }
    }

    private static void decodeOne(final ByteBuffer buffer, final MetaChangeCollector collector) {
        val type = toChangeType(buffer.getShort());

        switch (type) {
        case SEQUENCE_NUMBER_SEEN:
        case FILE_NUMBER_SEEN:
            decodeNumberChange(type, buffer, collector);
            break;

        case ADD_FILE:
        case REMOVE_FILE:
            decodeFileChange(type, buffer, collector);
            break;
        }
    }

    private static ChangeType toChangeType(final short type) {
        return ChangeType.values()[type];
    }

    private static void decodeFileChange(final ChangeType type, final ByteBuffer buffer, final MetaChangeCollector collector) {
        val number = buffer.getLong();

        if (type == ChangeType.FILE_NUMBER_SEEN) {
            collector.setFileNumberSeen(number);
        } else {
            collector.setSequenceNumberSeen(number);
        }
    }

    private static void decodeNumberChange(final ChangeType type, final ByteBuffer buffer, final MetaChangeCollector collector) {
        val level = toFileLevel(buffer.getShort());
        val fileNumber = buffer.getLong();

        if (isDeletion(type)) {
            collector.removeDataFiles(level, fileNumber);
        } else {
            collector.addDataFiles(level, fileNumber);
        }
    }

    private static boolean isDeletion(final ChangeType type) {
        return type == ChangeType.REMOVE_FILE;
    }

    private static FileLevel toFileLevel(final short s) {
        return FileLevel.values()[s];
    }
}
