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
package org.apache.pulsar.utils;

import io.netty.buffer.ByteBuf;
import java.util.Stack;
import org.apache.pulsar.common.util.SimpleTextOutputStream;

public class StatsOutputStream extends SimpleTextOutputStream {
    private final Stack<Boolean> separators = new Stack<>();

    public StatsOutputStream(ByteBuf buffer) {
        super(buffer);
    }

    public StatsOutputStream startObject() {
        checkSeparator();
        separators.push(Boolean.FALSE);
        write('{');
        return this;
    }

    public StatsOutputStream startObject(String key) {
        checkSeparator();
        write('"').writeEncoded(key).write("\":{");
        separators.push(Boolean.FALSE);
        return this;
    }

    public StatsOutputStream endObject() {
        separators.pop();
        write('}');
        return this;
    }

    public StatsOutputStream startList() {
        checkSeparator();
        separators.push(Boolean.FALSE);
        write('[');
        return this;
    }

    public StatsOutputStream startList(String key) {
        checkSeparator();
        write('"').writeEncoded(key).write("\":[");
        separators.push(Boolean.FALSE);
        return this;
    }

    public StatsOutputStream endList() {
        separators.pop();
        write(']');
        return this;
    }

    public StatsOutputStream writePair(String name, boolean value) {
        checkSeparator();
        write('"').writeEncoded(name).write("\":").write(value);
        return this;
    }

    public StatsOutputStream writePair(String name, long n) {
        checkSeparator();
        write('"').writeEncoded(name).write("\":").write(n);
        return this;
    }

    public StatsOutputStream writePair(String name, double d) {
        checkSeparator();
        write('"').writeEncoded(name).write("\":").write(d);
        return this;
    }

    public StatsOutputStream writePair(String name, String s) {
        checkSeparator();
        write('"').writeEncoded(name).write("\":\"").writeEncoded(s).write('"');
        return this;
    }

    public StatsOutputStream writeItem(boolean value) {
        checkSeparator();
        super.write(value);
        return this;
    }

    public StatsOutputStream writeItem(long n) {
        checkSeparator();
        super.write(n);
        return this;
    }

    public StatsOutputStream writeItem(double d) {
        checkSeparator();
        super.write(d);
        return this;
    }

    StatsOutputStream writeItem(String s) {
        checkSeparator();

        write('"').writeEncoded(s).write('"');
        return this;
    }

    private void checkSeparator() {
        if (separators.isEmpty()) {
            return;
        } else if (separators.peek() == Boolean.TRUE) {
            write(",");
        } else {
            separators.set(separators.size() - 1, Boolean.TRUE);
        }
    }
}
