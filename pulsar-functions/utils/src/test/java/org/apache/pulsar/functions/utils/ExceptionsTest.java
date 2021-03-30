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

package org.apache.pulsar.functions.utils;

import static org.apache.pulsar.functions.utils.Exceptions.rethrowIOException;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import org.testng.annotations.Test;

/**
 * Unit test of {@link Exceptions}.
 */
public class ExceptionsTest {

    @Test
    public void testRethrowIOException() {
        IOException ioe = new IOException("test");
        try {
            rethrowIOException(ioe);
            fail("Should rethrow IOException");
        } catch (IOException e) {
            assertSame(ioe, e);
        }
    }

    @Test
    public void testRethrowRuntimeExceptionAsIOException() throws IOException {
        RuntimeException re = new RuntimeException("test");
        try {
            rethrowIOException(re);
            fail("Should rethrow RuntimeException");
        } catch (RuntimeException e) {
            assertSame(re, e);
        }
    }

    @Test
    public void testRethrowErrorAsIOException() throws IOException {
        Error error = new Error("test");
        try {
            rethrowIOException(error);
            fail("Should rethrow Error");
        } catch (Error e) {
            assertSame(error, e);
        }
    }

    @Test
    public void testRethrowOtherExceptionAsIOException() throws IOException {
        Exception e = new Exception("test");
        try {
            rethrowIOException(e);
            fail("Should rethrow IOException");
        } catch (IOException ioe) {
            assertEquals("test", ioe.getMessage());
            assertSame(e, ioe.getCause());
        }
    }

    @Test
    public void testAreExceptionsPresentInChain() {
        assertFalse(Exceptions.areExceptionsPresentInChain(null, IllegalStateException.class));
    }

    @Test
    public void testAreExceptionsPresentInChain2() {
        assertTrue(Exceptions.areExceptionsPresentInChain(new IllegalStateException(), IllegalStateException.class));
    }

    @Test
    public void testAreExceptionsPresentInChain3() {
        assertTrue(Exceptions.areExceptionsPresentInChain(new IllegalArgumentException(new IllegalStateException()), IllegalStateException.class));
    }

    @Test
    public void testAreExceptionsPresentInChain4() {
        assertTrue(Exceptions.areExceptionsPresentInChain(new IllegalArgumentException(new IllegalStateException()), UnsupportedOperationException.class, IllegalStateException.class));
    }

    @Test
    public void testAreExceptionsPresentInChain5() {
        assertFalse(Exceptions.areExceptionsPresentInChain(new IllegalArgumentException(new IllegalArgumentException()), IllegalStateException.class));
    }

}
