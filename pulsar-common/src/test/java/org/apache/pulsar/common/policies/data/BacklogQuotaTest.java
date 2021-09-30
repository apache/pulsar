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
package org.apache.pulsar.common.policies.data;

import org.apache.pulsar.common.policies.data.BacklogQuota.RetentionPolicy;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BacklogQuotaTest {
    @Test
    public void testBacklogQuotaIdentity() {
        Assert.assertNotEquals(BacklogQuota.builder()
                        .limitSize(1)
                        .retentionPolicy(RetentionPolicy.producer_exception)
                        .build(),
                BacklogQuota.builder()
                        .limitSize(2)
                        .retentionPolicy(RetentionPolicy.producer_exception)
                        .build());
        Assert.assertNotEquals(BacklogQuota.builder()
                        .limitSize(1)
                        .retentionPolicy(RetentionPolicy.producer_exception)
                        .build(),
                BacklogQuota.builder()
                        .limitSize(2)
                        .retentionPolicy(RetentionPolicy.consumer_backlog_eviction)
                        .build());
        Assert.assertNotEquals(BacklogQuota.builder()
                        .limitSize(2)
                        .retentionPolicy(RetentionPolicy.producer_exception)
                        .build(),
                BacklogQuota.builder()
                        .limitSize(1)
                        .retentionPolicy(RetentionPolicy.consumer_backlog_eviction)
                        .build());
        Assert.assertEquals(BacklogQuota.builder()
                        .limitSize(10)
                        .retentionPolicy(RetentionPolicy.producer_exception)
                        .build(),
                BacklogQuota.builder()
                        .limitSize(10)
                        .retentionPolicy(RetentionPolicy.producer_exception)
                        .build());
        BacklogQuota quota1 = BacklogQuota.builder()
                .limitSize(10)
                .retentionPolicy(RetentionPolicy.producer_exception)
                .build();
        BacklogQuota quota2 = BacklogQuota.builder()
                .limitSize(10)
                .retentionPolicy(RetentionPolicy.producer_exception)
                .build();
        Assert.assertEquals(quota1.hashCode(), quota2.hashCode());
    }
}
