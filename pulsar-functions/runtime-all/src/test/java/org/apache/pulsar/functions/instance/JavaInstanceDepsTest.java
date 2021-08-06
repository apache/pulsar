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

package org.apache.pulsar.functions.instance;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Slf4j
/**
 * This test serves to make sure that the correct classes are included in the java-instance.jar
 * THAT JAR SHOULD ONLY CONTAIN THE INTERFACES THAT PULSAR FUNCTION'S FRAMEWORK USES TO INTERACT WITH USER CODE
 * WHICH INCLUDES CLASSES FROM THE FOLLOWING LIBRARIES
 *     1. pulsar-io-core
 *     2. pulsar-functions-api
 *     3. pulsar-client-api
 *     4. slf4j-api
 *     5. log4j-slf4j-impl
 *     6. log4j-api
 *     7. log4j-core
 */
public class JavaInstanceDepsTest {

    @Test
    public void testInstanceJarDeps() throws IOException {
        File jar = new File("target/java-instance.jar");
        
        @Cleanup
        ZipInputStream zip = new ZipInputStream(jar.toURI().toURL().openStream());

        List<String> notAllowedClasses = new LinkedList<>();
        while(true) {
            ZipEntry e = zip.getNextEntry();
            if (e == null)
                break;
            String name = e.getName();
            if (name.endsWith(".class") && !name.startsWith("META-INF")) {
                // The only classes in the java-instance.jar should be org.apache.pulsar, slf4j, and log4j classes
                // filter out those classes to see if there are any other classes that should not be allowed
                if (!name.startsWith("org/apache/pulsar")
                        && !name.startsWith("org/slf4j")
                        && !name.startsWith("org/apache/logging/slf4j")
                        && !name.startsWith("org/apache/logging/log4j")) {
                    notAllowedClasses.add(name);
                }
            }
        }

        Assert.assertEquals(notAllowedClasses, Collections.emptyList(), notAllowedClasses.toString());
    }
}
