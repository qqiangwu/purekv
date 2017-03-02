package me.wuqq.log;

import lombok.SneakyThrows;
import lombok.val;
import me.wuqq.core.OperationLogReader;
import me.wuqq.support.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Optional;

@NotThreadSafe
public class OperationLogReaderImpl implements OperationLogReader {
    private static int DEFAULT_CONTENT_BUFFER_SIZE = 1024;

    @NotNull
    private final ReadableByteChannel mInput;

    @NotNull
    private final ByteBuffer mHeaderBuffer;

    @NotNull
    private ByteBuffer mContentBuffer;

    public OperationLogReaderImpl(final ReadableByteChannel reader) {
        mInput = reader;
        mHeaderBuffer = ByteBuffer.allocate(Integer.BYTES);
        mContentBuffer = ByteBuffer.allocateDirect(DEFAULT_CONTENT_BUFFER_SIZE);
    }

    @Override
    public Optional<byte[]> read() {
        ensureOpened();

        val len = readLength();

        if (len == 0) {
            return Optional.empty();
        } else {
            return Optional.of(readContent(len));
        }
    }

    private void ensureOpened() {
        if (!mInput.isOpen()) {
            throw new RuntimeException("Channel had already been closed");
        }
    }

    @SneakyThrows
    private int readLength() {
        mHeaderBuffer.clear();

        if (isEof(mInput.read(mHeaderBuffer))) {
            return 0;
        } else {
            mHeaderBuffer.flip();
            return mHeaderBuffer.getInt();
        }
    }

    @SneakyThrows
    private byte[] readContent(final int len) {
        prepareContentBufferOfSize(len);

        int nRead = 0;

        while (nRead < len) {
            val n = mInput.read(mContentBuffer);

            if (isEof(n)) {
                throw new LogCorruptionException("Log corrupted");
            }

            nRead += n;
        }

        mContentBuffer.flip();

        return mContentBuffer.array();
    }

    private void prepareContentBufferOfSize(final int len) {
        mContentBuffer.clear();

        if (mContentBuffer.capacity() < len) {
            mContentBuffer = ByteBuffer.allocateDirect(len);
        }
    }

    private boolean isEof(final int nRead) {
        return nRead <= 0;
    }

    @Override
    public void close() throws Exception {
        mInput.close();
    }
}
