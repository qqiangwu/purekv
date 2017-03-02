package me.wuqq.core;

import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.util.List;

public interface Env {
    ReadableByteChannel newReadableChannel(Path fileName);
    WritableByteChannel newWritableChannel(Path fileName);

    boolean fileExists(Path fileName);
    List<Path> listDir(Path fileName);

    String readAsString(Path fileName);
}
