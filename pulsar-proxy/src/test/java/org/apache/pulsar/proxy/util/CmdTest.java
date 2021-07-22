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
package org.apache.pulsar.proxy.util;

import static org.testng.Assert.assertTrue;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import org.apache.pulsar.common.configuration.FieldContext;
import org.testng.annotations.Test;

public class CmdTest {

    @Test
    public void cmdParserProxyConfigurationTest() throws Exception {
        String value = generateDoc("org.apache.pulsar.proxy.server.ProxyConfiguration");
        assertTrue(value.contains("Pulsar proxy"));
    }

    private String generateDoc(String clazz) throws Exception {
        PrintStream oldStream = System.out;
        try (ByteArrayOutputStream baoStream = new ByteArrayOutputStream(2048);
             PrintStream cacheStream = new PrintStream(baoStream);) {
            System.setOut(cacheStream);
            CmdGenerateDocumentation.main(("-c " + clazz).split(" "));
            String message = baoStream.toString();
            Class cls = Class.forName(clazz);
            Field[] fields = cls.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                FieldContext fieldContext = field.getAnnotation(FieldContext.class);
                if (fieldContext == null) {
                    continue;
                }
                assertTrue(message.indexOf(field.getName()) > 0);
            }
            return message;
        } finally {
            System.setOut(oldStream);
        }
    }
}
