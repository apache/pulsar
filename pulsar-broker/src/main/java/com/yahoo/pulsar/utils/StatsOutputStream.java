/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.utils;

import java.util.Stack;

import io.netty.buffer.ByteBuf;

public class StatsOutputStream {
    private final ByteBuf buffer;
    private final Stack<Boolean> separators = new Stack<>();
    private final static char[] hexChars = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e',
            'f' };

    public StatsOutputStream(ByteBuf buffer) {
        this.buffer = buffer;
    }

    public StatsOutputStream startObject() {
        checkSeparator();
        separators.push(Boolean.FALSE);
        return writeString("{");
    }

    public StatsOutputStream startObject(String key) {
        checkSeparator();
        writeChar('"').writeEncodedString(key).writeString("\":{");
        separators.push(Boolean.FALSE);
        return this;
    }

    public StatsOutputStream endObject() {
        separators.pop();
        return writeString("}");
    }

    public StatsOutputStream startList() {
        checkSeparator();
        separators.push(Boolean.FALSE);
        return writeString("[");
    }

    public StatsOutputStream startList(String key) {
        checkSeparator();
        writeChar('"').writeEncodedString(key).writeString("\":[");
        separators.push(Boolean.FALSE);
        return this;
    }

    public StatsOutputStream endList() {
        separators.pop();
        return writeString("]");
    }

    public StatsOutputStream writePair(String name, boolean value) {
        checkSeparator();
        return writeChar('"').writeEncodedString(name).writeString("\":").writeBoolean(value);
    }

    public StatsOutputStream writePair(String name, long n) {
        checkSeparator();
        return writeChar('"').writeEncodedString(name).writeString("\":").writeLong(n);
    }

    public StatsOutputStream writePair(String name, double d) {
        checkSeparator();
        return writeChar('"').writeEncodedString(name).writeString("\":").writeDouble(d);
    }

    public StatsOutputStream writePair(String name, String s) {
        checkSeparator();
        return writeChar('"').writeEncodedString(name).writeString("\":\"").writeEncodedString(s).writeChar('"');
    }

    public StatsOutputStream write(boolean value) {
        checkSeparator();
        return writeBoolean(value);
    }

    public StatsOutputStream write(long n) {
        checkSeparator();
        return writeLong(n);
    }

    public StatsOutputStream write(double d) {
        checkSeparator();
        return writeDouble(d);
    }

    StatsOutputStream write(String s) {
        checkSeparator();
        return writeChar('"').writeEncodedString(s).writeChar('"');
    }

    private void checkSeparator() {
        if (separators.isEmpty()) {
            return;
        } else if (separators.peek() == Boolean.TRUE) {
            writeString(",");
        } else {
            separators.set(separators.size() - 1, Boolean.TRUE);
        }
    }

    StatsOutputStream writeBytes(byte[] a, int offset, int len) {
        buffer.writeBytes(a, offset, len);
        return this;
    }

    StatsOutputStream writeChar(char c) {
        buffer.writeByte((byte) c);
        return this;
    }

    StatsOutputStream writeString(String s) {
        if (s == null) {
            return this;
        }
        int len = s.length();
        for (int i = 0; i < len; i++) {
            buffer.writeByte((byte) s.charAt(i));
        }

        return this;
    }

    StatsOutputStream writeEncodedString(String s) {
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

    StatsOutputStream writeBoolean(boolean b) {
        writeString(b ? "true" : "false");
        return this;
    }

    StatsOutputStream writeLong(long n) {
        NumberFormat.format(this.buffer, n);
        return this;
    }

    StatsOutputStream writeDouble(double d) {
        long i = (long) d;
        writeLong(i);

        long r = Math.abs((long) (1000 * (d - i)));
        writeString(".");
        if (r == 0) {
            writeString("0");
            return this;
        }

        if (r < 100) {
            writeString("0");
        }

        if (r < 10) {
            writeString("0");
        }

        writeLong(r);
        return this;
    }

}
