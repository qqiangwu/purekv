package me.wuqq.log;

import lombok.SneakyThrows;
import lombok.val;
import me.wuqq.core.OperationLogAppender;
import me.wuqq.support.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Log files are composed of 32KB blocks. Each block contains multiple records and an optional padding trailer.
 *
 *  block := record* trailer?
 *  record :=
 *      length: uint16
 *      checksum: uint32	// crc32c of type and data[]
 *      type: uint8		// One of FULL, FIRST, MIDDLE, LAST
 *      data: uint8[length]
 *
 * The above is the log format used in LevelDB. For simplicity, I use a extremely simple format here.
 *
 * log := record*
 * record :=
 *      length: uint32
 *      data: uint8[length]
 *
 */
@NotThreadSafe
public class OperationLogAppenderImpl implements OperationLogAppender {
    @NotNull
    private final WritableByteChannel mOutput;

    @NotNull
    private final ByteBuffer mHeaderBuffer;

    private int mSize;

    public OperationLogAppenderImpl(final WritableByteChannel outputChannel) {
        mOutput = outputChannel;
        mHeaderBuffer = ByteBuffer.allocate(4);
    }

    @Override
    public void sync() {
        /* empty for now */
        ensureOpened();
    }

    @Override
    public long size() {
        return mSize;
    }

    @Override
    public void append(final byte[] frame) {
        ensureOpened();

        val length = frame.length;

        if (length != 0) {
            mSize += writeHeader(frame);
            mSize += writeContent(frame);
        }
    }

    @SneakyThrows
    private int writeContent(final byte[] frame) {
        mOutput.write(ByteBuffer.wrap(frame));

        return frame.length;
    }

    @SneakyThrows
    private int writeHeader(final byte[] frame) {
        mHeaderBuffer.clear();
        mHeaderBuffer.putInt(frame.length);
        mHeaderBuffer.flip();

        mOutput.write(mHeaderBuffer);

        return Integer.BYTES;
    }

    private void ensureOpened() {
        if (!mOutput.isOpen()) {
            throw new RuntimeException("Output had already been closed");
        }
    }

    @Override
    public void close() throws Exception {
        mOutput.close();
    }
}
