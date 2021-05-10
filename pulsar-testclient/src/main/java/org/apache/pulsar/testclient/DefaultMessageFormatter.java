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
package org.apache.pulsar.testclient;

import java.nio.charset.StandardCharsets;
import java.util.Random;

public class DefaultMessageFormatter implements IMessageFormatter {
    Random r  = new Random();


    @Override
    public byte[] formatMessage(String producerName, long msgId, byte[] message) {
        String sMessage = new String(message, StandardCharsets.UTF_8);
        if (producerName != null && !producerName.isEmpty()) {
            sMessage = sMessage.replaceAll("%p", producerName);
        }
        sMessage = sMessage.replaceAll("%i", String.valueOf(msgId));
        sMessage = sMessage.replaceAll("%t", String.valueOf(System.nanoTime()));

        int idx = sMessage.indexOf("%");
        while (idx > 0) {

            float size = 0;
            int i=1;
            for (; idx+i < sMessage.length(); i++) {
                char c = sMessage.charAt(idx + i);
                if (Character.isDigit(c) && c != '.') {
                    continue;
                }
                if (c == '.' || c == '-') {
                    continue;
                }
                break;
            }
            if (i != 1) {
                size = Float.valueOf(new String(sMessage.substring(idx+1,idx+i)));
            }

            String sub = sMessage.substring(idx, idx+i+1);

            if (sMessage.charAt(idx+i) == 'f') {
                sMessage=sMessage.replaceFirst(sub, getFloatValue(size));
            } else if (sMessage.charAt(idx+i) == 'l') {
                sMessage=sMessage.replaceFirst(sub, getLongValue(size));
            } else if (sMessage.charAt(idx+i) == 'd') {
                sMessage = sMessage.replaceFirst(sub, getIntValue(size));
            } else if (sMessage.charAt(idx+i) == 's') {
                sMessage = sMessage.replaceFirst(sub, getStringValue(size));
            }
            idx = sMessage.indexOf("%", idx);
        }
        return sMessage.getBytes(StandardCharsets.UTF_8);
    }

    private float _getFloatValue(float size) {
        float f = r.nextFloat();
        int mag = (int) Math.abs(size);
        f = f * (float) Math.pow(10, mag);
        if (size < 0 && ((int) f) % 2 == 1) {
            return f * -1;
        }
        return f;
    }

    private String getStringValue(float size) {
        int s = (int) size;
        if (size == 0) {
            size = 20;
        };
        String result = "";
        for(int i = 0; i < s; i++) {
            result = result + (char) ((int) 'a' + (int) (r.nextFloat() * 26));
        }
        return result;
    }

    private String getFloatValue(float size) {
        if (size == 0) {
            return String.valueOf(r.nextFloat());
        }
        String format = "%" + String.valueOf(size) + "f";

        return String.format(format, _getFloatValue(size));
    }

    private String getIntValue(float size) {
        int i = 0;
        if (size != 0) {
            i = (int) _getFloatValue(size);
        }
        if (i == 0) {
            i = r.nextInt() + 1;
        }
        return String.valueOf(i);
    }
    private String getLongValue(float size) {
        if (size == 0) {
            return String.valueOf(r.nextLong());
        }
        return String.valueOf((long) _getFloatValue(size));
    }
}
