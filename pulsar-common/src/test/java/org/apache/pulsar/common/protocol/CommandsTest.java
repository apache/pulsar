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
package org.apache.pulsar.common.protocol;

import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CommandsTest {

    @Test
    public void initBatchMessageMetadataTest() {
        MessageMetadata messageMetadata = new MessageMetadata();
        MessageMetadata builder = new MessageMetadata();
        builder.setPublishTime(System.currentTimeMillis());
        builder.setProducerName("p1");
        builder.setSequenceId(1);
        builder.addReplicateTo("rep1");
        builder.addReplicateTo("rep2");
        builder.addProperty().setKey("key1").setValue("value1");
        builder.addProperty().setKey("key2").setValue("value2");
        Commands.initBatchMessageMetadata(messageMetadata, builder);
        Assert.assertEquals(2, messageMetadata.getPropertiesCount());
        Assert.assertEquals(2, messageMetadata.getReplicateTosCount());
    }
}
