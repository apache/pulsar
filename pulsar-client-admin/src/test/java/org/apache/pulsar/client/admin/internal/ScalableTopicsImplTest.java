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
package org.apache.pulsar.client.admin.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import jakarta.ws.rs.client.WebTarget;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.testng.annotations.Test;

public class ScalableTopicsImplTest {

    /** Missing the local name: not a valid topic name. */
    private static final String MALFORMED_TOPIC = "topic://tenant/ns";

    /** The asynchronous permission methods report an invalid topic name through the future, never by throwing. */
    @Test
    public void testPermissionMethodsFailTheFutureOnMalformedTopic() {
        ScalableTopicsImpl scalableTopics = new ScalableTopicsImpl(mock(WebTarget.class), null, 0);

        assertFailsWithIllegalArgument(scalableTopics.getPermissionsAsync(MALFORMED_TOPIC));
        assertFailsWithIllegalArgument(
                scalableTopics.grantPermissionAsync(MALFORMED_TOPIC, "role", Set.of(AuthAction.produce)));
        assertFailsWithIllegalArgument(scalableTopics.revokePermissionsAsync(MALFORMED_TOPIC, "role"));
    }

    private static void assertFailsWithIllegalArgument(CompletableFuture<?> future) {
        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join).hasCauseInstanceOf(IllegalArgumentException.class);
    }
}
