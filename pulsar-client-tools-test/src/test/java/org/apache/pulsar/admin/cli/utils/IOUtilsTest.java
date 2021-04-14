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
package org.apache.pulsar.admin.cli.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class IOUtilsTest {

    InputStream stdin = System.in;
    PrintStream stdout = System.out;

    @AfterClass(alwaysRun = true)
    public void tearDown() {
        System.setIn(stdin);
        System.setOut(stdout);
    }

    @BeforeClass
    public void setUp() {
    }

    @Test
    public void test1() {
        String data = "y";
        try {
            System.setIn(new ByteArrayInputStream(data.getBytes()));
            Assert.assertTrue(IOUtils.confirmPrompt("Is you name John Doe?"));
        } catch (IOException e) {
            Assert.fail();
        }
    }

    @Test
    public void test2() {
        String data = "yes";
        try {
            System.setIn(new ByteArrayInputStream(data.getBytes()));
            Assert.assertTrue(IOUtils.confirmPrompt("Are we there yet?"));
        } catch (IOException e) {
            Assert.fail();
        }
    }

    @Test
    public void test3() {
        String data = "n";
        try {
            System.setIn(new ByteArrayInputStream(data.getBytes()));
            Assert.assertFalse(IOUtils.confirmPrompt("Can I go home?"));
        } catch (IOException e) {
            Assert.fail();
        }
    }

    @Test
    public void test4() {
        String data = "no";
        try {
            System.setIn(new ByteArrayInputStream(data.getBytes()));
            Assert.assertFalse(IOUtils.confirmPrompt("Is it Sunday?"));
        } catch (IOException e) {
            Assert.fail();
        }
    }

    // TODO: Disabled because it fails with maven surefire 2.20
    // https://github.com/apache/pulsar/issues/766
    @Test(enabled = false, description = "Should go into infinte loop since j is an invalid response.")
    public void test5() {
        try {
            String data = "j";
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            System.setOut(new PrintStream(baos));
            System.setIn(new ByteArrayInputStream(data.getBytes()));
            @Cleanup("shutdownNow")
            ExecutorService executor = Executors.newSingleThreadExecutor();
            @SuppressWarnings("unchecked")
            Future<Void> future = (Future<Void>) executor.submit(() -> {
                try {
                    IOUtils.confirmPrompt("When will this week end?");
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            });
            future.get(1, TimeUnit.SECONDS);
            Assert.fail();
        } catch (Exception ex) {
            // expected
        }
    }

    // TODO: Disabled because it fails with maven surefire 2.20
    // https://github.com/apache/pulsar/issues/766
    @Test(enabled = false, description = "Should go into infinte loop since \r means next line.")
    public void test6() {
        try {
            String data = "\n";
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            System.setOut(new PrintStream(baos));
            System.setIn(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)));
            @Cleanup("shutdownNow")
            ExecutorService executor = Executors.newSingleThreadExecutor();
            @SuppressWarnings("unchecked")
            Future<Void> future = (Future<Void>) executor.submit(() -> {
                try {
                    IOUtils.confirmPrompt("Is you name Pulsar?");
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            });
            future.get(1, TimeUnit.SECONDS);
            Assert.fail();
        } catch (Exception ex) {
            // expected
        }
    }
}
