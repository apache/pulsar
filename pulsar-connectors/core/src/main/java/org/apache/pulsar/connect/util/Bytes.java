package org.apache.pulsar.connect.util;

public class Bytes {

    public static final long MB = 1024L * 1024L;

    public static double toMb(long size) {
        return (double) size / MB;
    }

    private Bytes() {}
}
