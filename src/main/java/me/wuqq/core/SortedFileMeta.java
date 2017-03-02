package me.wuqq.core;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SortedFileMeta {
    private long fileID;
    private long fileSize;
    private byte[] smallestKey;
    private byte[] largestKey;
}
