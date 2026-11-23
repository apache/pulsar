/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.stats.prometheus;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.pulsar.common.util.SimpleTextOutputStream;

/**
 * Adapts the official writer's underscore-escaped text format to Pulsar: TYPE-only metadata and
 * a cluster label on every sample. Metric names, values, types and escaping remain the client's job.
 * Complete lines pass directly from the writer's byte array; only split lines need buffering.
 */
final class PulsarPrometheusTextOutput extends OutputStream {
    private static final byte[] TYPE = "# TYPE ".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] CLUSTER = "cluster=".getBytes(StandardCharsets.US_ASCII);
    private final SimpleTextOutputStream stream;
    private final byte[] clusterLabel;
    private byte[] pending = new byte[256];
    private int size;

    PulsarPrometheusTextOutput(SimpleTextOutputStream stream, String cluster) {
        this.stream = stream;
        clusterLabel = ("cluster=\"" + escapeLabelValue(cluster) + "\"").getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void write(int value) {
        ensureCapacity(size + 1);
        pending[size++] = (byte) value;
        if (value == '\n') {
            writeLine(pending, 0, size);
            size = 0;
        }
    }

    @Override
    public void write(byte[] data, int offset, int length) {
        int start = offset;
        int end = offset + length;
        for (int i = offset; i < end; i++) {
            if (data[i] == '\n') {
                if (size == 0) {
                    writeLine(data, start, i + 1);
                } else {
                    append(data, start, i + 1 - start);
                    writeLine(pending, 0, size);
                    size = 0;
                }
                start = i + 1;
            }
        }
        if (start < end) {
            append(data, start, end - start);
        }
    }

    private void append(byte[] data, int offset, int length) {
        ensureCapacity(size + length);
        System.arraycopy(data, offset, pending, size, length);
        size += length;
    }

    private void ensureCapacity(int capacity) {
        if (capacity > pending.length) {
            pending = Arrays.copyOf(pending, Math.max(capacity, pending.length * 2));
        }
    }

    @Override
    public void flush() {
        // The writer flushes once at the end; every line it produces is newline-terminated.
        if (size > 0) {
            throw new IllegalStateException("Unterminated exposition line: "
                    + new String(pending, 0, Math.min(size, 80), StandardCharsets.UTF_8));
        }
    }

    private void writeLine(byte[] data, int start, int end) {
        if (data[start] == '#') {
            if (end - start >= TYPE.length && Arrays.equals(data, start, start + TYPE.length, TYPE, 0, TYPE.length)) {
                stream.write(data, start, end - start);
            }
            return;
        }
        int nameEnd = start;
        while (nameEnd < end && data[nameEnd] != '{' && data[nameEnd] != ' ') {
            nameEnd++;
        }
        if (nameEnd == end) {
            throw new IllegalStateException("Malformed exposition line: "
                    + new String(data, start, Math.min(end - start, 80), StandardCharsets.UTF_8));
        }
        boolean hasLabels = data[nameEnd] == '{';
        if (hasLabels && hasClusterLabel(data, nameEnd + 1, end)) {
            stream.write(data, start, end - start);
            return;
        }
        stream.write(data, start, nameEnd - start).write('{').write(clusterLabel);
        if (hasLabels) {
            if (data[nameEnd + 1] != '}') {
                stream.write(',');
            }
            stream.write(data, nameEnd + 1, end - nameEnd - 1);
        } else {
            stream.write('}').write(data, nameEnd, end - nameEnd);
        }
    }

    private static boolean hasClusterLabel(byte[] data, int start, int end) {
        boolean quoted = false;
        for (int i = start; i < end; i++) {
            if (!quoted && (i == start || data[i - 1] == ',') && end - i >= CLUSTER.length
                    && Arrays.equals(data, i, i + CLUSTER.length, CLUSTER, 0, CLUSTER.length)) {
                return true;
            }
            if (quoted && data[i] == '\\') {
                i++;
            } else if (data[i] == '"') {
                quoted = !quoted;
            } else if (!quoted && data[i] == '}') {
                return false;
            }
        }
        return false;
    }

    static String escapeLabelValue(String value) {
        return value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }
}
