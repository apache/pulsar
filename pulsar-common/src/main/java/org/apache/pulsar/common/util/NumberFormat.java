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
 * Custom number formatter for {@code io.netty.buffer.ByteBuf}.
 */
public class NumberFormat {

    static void format(ByteBuf out, long num) {
        if (num == 0) {
            out.writeByte('0');
            return;
        }

        // Long.MIN_VALUE needs special handling since abs(Long.MIN_VALUE) = abs(Long.MAX_VALUE) + 1
        boolean encounteredMinValue = (num == Long.MIN_VALUE);
        if (num < 0) {
            out.writeByte('-');
            num += encounteredMinValue ? 1 : 0;
            num *= -1;
        }

        // Putting the number in bytebuf in reverse order
        int start = out.writerIndex();
        formatHelper(out, num);
        int end = out.writerIndex();

        if (encounteredMinValue) {
            out.setByte(start, out.getByte(start) + 1);
        }

        // Reversing the digits
        end--;
        for (int i = 0; i <= (end - start) / 2; i++) {
            byte tmp = out.getByte(end - i);
            out.setByte(end - i, out.getByte(start + i));
            out.setByte(start + i, tmp);
        }
    }

    static void formatHelper(ByteBuf out, long num) {
        while (num != 0) {
            out.writeByte((int) ('0' + num % 10));
            num /= 10;
        }
    }
}
