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
package org.apache.pulsar.client.admin.utils;

import java.lang.reflect.Constructor;
import lombok.experimental.UtilityClass;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.OffloadProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.MessageId;

/**
 * Helper class for class instantiations and it also contains methods to work with schemas.
 */
@SuppressWarnings("unchecked")
@UtilityClass
public class DefaultImplementation {
    private static final Class<PulsarAdminBuilder> ADMIN_CLIENT_BUILDER_IMPL = ReflectionUtils.newClassInstance(
            "org.apache.pulsar.client.admin.internal.PulsarAdminBuilderImpl");

    private static final Constructor<OffloadProcessStatus> OFFLOAD_PROCESS_STATUS_IMPL_status_string_messageid =
            ReflectionUtils.getConstructor(
            "org.apache.pulsar.client.admin.internal.OffloadProcessStatusImpl",
                    LongRunningProcessStatus.Status.class, String.class, MessageId.class);

    public static PulsarAdminBuilder newAdminClientBuilder() {
        return ReflectionUtils.catchExceptions(() -> ADMIN_CLIENT_BUILDER_IMPL.getDeclaredConstructor().newInstance());
    }

    public static OffloadProcessStatus newOffloadProcessStatus(LongRunningProcessStatus.Status status, String lastError
            , MessageId messageId) {
        return ReflectionUtils.catchExceptions(() -> OFFLOAD_PROCESS_STATUS_IMPL_status_string_messageid.newInstance(
                status, lastError, messageId));
    }

}
