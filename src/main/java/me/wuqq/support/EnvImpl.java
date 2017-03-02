package me.wuqq.support;

import lombok.SneakyThrows;
import lombok.val;
import me.wuqq.core.Env;

import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Collectors;

@ThreadSafe
public class EnvImpl implements Env {
    private static final EnvImpl INSTANCE = new EnvImpl();

    public static final synchronized Env getInstance() {
        return INSTANCE;
    }

    private EnvImpl() {}

    @Override
    @SneakyThrows
    public ReadableByteChannel newReadableChannel(final Path fileName) {
        return FileChannel.open(fileName, StandardOpenOption.READ);
    }

    @Override
    @SneakyThrows
    public WritableByteChannel newWritableChannel(final Path fileName) {
        return FileChannel.open(fileName, StandardOpenOption.APPEND);
    }

    @Override
    public boolean fileExists(final Path file) {
        return Files.exists(file);
    }

    @Override
    @SneakyThrows
    public List<Path> listDir(final Path dir) {
        return Files.list(dir).collect(Collectors.toList());
    }

    @Override
    @SneakyThrows
    public String readAsString(Path fileName) {
        val bytes = Files.readAllBytes(fileName);
        return new String(bytes, StandardCharsets.US_ASCII);
    }

}
