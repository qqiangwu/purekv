package me.wuqq.memtable;

import lombok.SneakyThrows;
import lombok.val;
import me.wuqq.core.MemoryTable.Record;
import me.wuqq.support.NotThreadSafe;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

// record :=
//      type: uint8  // 0 for insertion and 1 for deletion
//      seq:  uint64
//      data: uint8[len]
//
// if type == 0
//  data :=
//      keylen: uint32
//      key: uint8[keylen]
//      vallen: uint32
//      val: uint8[vallen]
// if type == 1
//  data :=
//      keylen: uint32
//      key: uint8[keylen]
@NotThreadSafe
public final class MemoryTableLogFormatter {
    private static final int RECORD_VALUE = 0;
    private static final int RECORD_DELETE = 1;

    private final ByteArrayOutputStream mBuffer = new ByteArrayOutputStream();
    private final ByteBuffer mByteBuffer = ByteBuffer.allocate(Long.BYTES);

    @SneakyThrows
    public byte[] encodeRecord(final long sequenceNumber, final byte[] key, final byte[] value) {
        val isDeleteRecord = isDeletion(value);
        val type = isDeleteRecord? RECORD_DELETE: RECORD_VALUE;

        mBuffer.reset();
        mBuffer.write(type);
        mBuffer.write(toBytes(sequenceNumber));
        mBuffer.write(toBytes(key.length));
        mBuffer.write(key);

        if (type == RECORD_VALUE) {
            mBuffer.write(toBytes(value.length));
            mBuffer.write(value);
        }

        return mBuffer.toByteArray();
    }

    private byte[] toBytes(final long v) {
        mByteBuffer.clear();
        mByteBuffer.putLong(v);
        mByteBuffer.flip();
        return mByteBuffer.array();
    }

    private byte[] toBytes(final int v) {
        mByteBuffer.clear();
        mByteBuffer.putInt(v);
        mByteBuffer.flip();
        return mByteBuffer.array();
    }

    private boolean isDeletion(final byte[] value) {
        return value == null;
    }

    public static Record decodeRecord(final byte[] raw) {
        val buffer = ByteBuffer.wrap(raw);
        val type = buffer.get();

        if (type == RECORD_VALUE) {
            return decodeValueRecord(buffer);
        } else if (type == RECORD_DELETE) {
            return decodeDeleteRecord(buffer);
        } else {
            throw new RuntimeException(String.format("Unknown record %d", type));
        }
    }

    private static Record decodeDeleteRecord(final ByteBuffer buffer) {
        val sequenceNumber = buffer.getLong();
        val key = readByteArray(buffer);

        return new Record(sequenceNumber, false, key, null);
    }

    private static Record decodeValueRecord(final ByteBuffer buffer) {
        val sequenceNumber = buffer.getLong();
        val key = readByteArray(buffer);
        val value = readByteArray(buffer);

        return new Record(sequenceNumber, true, key, value);
    }

    private static byte[] readByteArray(final ByteBuffer buffer) {
        val len = buffer.getInt();
        val array = new byte[len];

        buffer.get(array);

        return array;
    }
}
