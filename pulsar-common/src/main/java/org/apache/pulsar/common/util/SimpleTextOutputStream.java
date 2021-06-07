/**
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
package org.apache.pulsar.common.util;

import io.netty.buffer.ByteBuf;

/**
 * Format strings and numbers into a ByteBuf without any memory allocation.
 *
 */
public class SimpleTextOutputStream {
    private final ByteBuf buffer;
    private static final char[] hexChars = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e',
            'f' };

    public SimpleTextOutputStream(ByteBuf buffer) {
        this.buffer = buffer;
    }

    public SimpleTextOutputStream write(byte[] a) {
        buffer.writeBytes(a, 0, a.length);
        return this;
    }

    public SimpleTextOutputStream write(byte[] a, int offset, int len) {
        buffer.writeBytes(a, offset, len);
        return this;
    }

    public SimpleTextOutputStream write(char c) {
        buffer.writeByte((byte) c);
        return this;
    }

    public SimpleTextOutputStream write(String s) {
        if (s == null) {
            return this;
        }
        int len = s.length();
        for (int i = 0; i < len; i++) {
            buffer.writeByte((byte) s.charAt(i));
        }

        return this;
    }

    public SimpleTextOutputStream write(Number n) {
        if (n instanceof Integer) {
            return write(n.intValue());
        } else if (n instanceof Long) {
            return write(n.longValue());
        } else if (n instanceof Double) {
            return write(n.doubleValue());
        } else {
            return write(n.toString());
        }
    }

    public SimpleTextOutputStream writeEncoded(String s) {
        if (s == null) {
            return this;
        }

        int len = s.length();
        for (int i = 0; i < len; i++) {

            char c = s.charAt(i);
            if (c < 32 || c > 126) { // below 32 and above 126 in ascii table
                buffer.writeByte((byte) '\\');
                buffer.writeByte((byte) 'u');
                buffer.writeByte(hexChars[(c & 0xf000) >> 12]);
                buffer.writeByte(hexChars[(c & 0x0f00) >> 8]);
                buffer.writeByte(hexChars[(c & 0x00f0) >> 4]);
                buffer.writeByte(hexChars[c & 0x000f]);
                continue;
            }

            if (c == '\\' || c == '"') {
                buffer.writeByte((byte) '\\');
            }
            buffer.writeByte((byte) c);
        }
        return this;
    }

    public SimpleTextOutputStream write(boolean b) {
        write(b ? "true" : "false");
        return this;
    }

    public SimpleTextOutputStream write(long n) {
        NumberFormat.format(this.buffer, n);
        return this;
    }

    public SimpleTextOutputStream write(double d) {
        long i = (long) d;
        write(i);

        long r = Math.abs((long) (1000 * (d - i)));
        write('.');
        if (r == 0) {
            write('0');
            return this;
        }

        if (r < 100) {
            write('0');
        }

        if (r < 10) {
            write('0');
        }

        write(r);
        return this;
    }
}
