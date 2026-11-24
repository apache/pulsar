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
package org.apache.pulsar;

import static org.testng.Assert.assertTrue;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.Arrays;
import org.testng.annotations.Test;
import picocli.CommandLine.Option;

public class PulsarInitialNamespaceSetupTest {
    @Test
    public void testMainGenerateDocs() throws Exception {
        PrintStream oldStream = System.out;
        try {
            ByteArrayOutputStream baoStream = new ByteArrayOutputStream();
            System.setOut(new PrintStream(baoStream));

            Class argumentsClass =
                    Class.forName("org.apache.pulsar.PulsarInitialNamespaceSetup$Arguments");

            PulsarInitialNamespaceSetup.doMain(new String[]{"-cs", "cs", "-c", "c", "-g", "demo"});

            String message = baoStream.toString();

            Field[] fields = argumentsClass.getDeclaredFields();
            for (Field field : fields) {
                boolean fieldHasAnno = field.isAnnotationPresent(Option.class);
                if (fieldHasAnno) {
                    Option fieldAnno = field.getAnnotation(Option.class);
                    String[] names = fieldAnno.names();
                    if (names.length == 0) {
                        continue;
                    }
                    String nameStr = Arrays.asList(names).toString();
                    nameStr = nameStr.substring(1, nameStr.length() - 1);
                    assertTrue(message.indexOf(nameStr) > 0);
                }
            }
        } finally {
            System.setOut(oldStream);
        }
    }
}
