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
package org.apache.pulsar.client.api;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class PulsarClientExceptionTest {

    @Test
    public void testWrap() {
        IllegalArgumentException argumentException = new IllegalArgumentException("name is required");
        Throwable wrapped = PulsarClientException.wrap(argumentException, "check");
        assertThat(wrapped)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll("check", "name is required")
                .isInstanceOf(IllegalArgumentException.class)
                .matches(n -> {
                    assertThat(n.getStackTrace()).isEqualTo(argumentException.getStackTrace());
                    return true;
                })
                .hasNoCause();

        NullPointerException cause = new NullPointerException("name is null");
        IllegalArgumentException argumentExceptionWithCause =
                new IllegalArgumentException("name is required", cause);
        Throwable wrappedWithCause = PulsarClientException.wrap(argumentExceptionWithCause, "check");
        assertThat(wrappedWithCause)
                .isInstanceOf(IllegalArgumentException.class)
                .matches(n -> {
                    assertThat(n.getStackTrace()).isEqualTo(argumentExceptionWithCause.getStackTrace());
                    return true;
                })
                .hasMessageContainingAll("check", "name is required")
                .cause()
                .isEqualTo(cause);
    }

    @Test
    public void testUnwrapPulsarClientException() {
        PulsarClientException exception = new PulsarClientException("Pulsar client error");
        PulsarClientException result = PulsarClientException.unwrap(exception);
        assertThat(result).isEqualTo(exception);
    }

    @Test
    public void testUnwrapExecutionExceptionNestedException() {
        Exception originalException = new Exception("Nested cause");
        ExecutionException exception = new ExecutionException("Execution error", originalException);
        PulsarClientException result = PulsarClientException.unwrap(exception);
        assertThat(result)
                .isInstanceOf(PulsarClientException.class)
                .matches(n -> {
                    assertThat(n.getStackTrace()).isEqualTo(originalException.getStackTrace());
                    return true;
                })
                .cause()
                .isEqualTo(originalException);
    }

    @Test
    public void testUnwrapExecutionExceptionNestedPulsarClientException() {
        PulsarClientException originalException = new PulsarClientException("Nested cause");
        ExecutionException exception = new ExecutionException("Execution error", originalException);
        PulsarClientException result = PulsarClientException.unwrap(exception);
        assertThat(result)
                .isInstanceOf(PulsarClientException.class)
                .matches(n -> {
                    assertThat(n.getStackTrace()).isEqualTo(originalException.getStackTrace());
                    return true;
                })
                .isEqualTo(originalException);
    }
}
