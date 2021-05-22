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

package org.apache.bookkeeper.mledger.offload;

import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;


public class OffloadUtilsTest {

    @Test
    void testOffloadMetadataShouldClearBeforeSet() {
        MLDataFormats.ManagedLedgerInfo.LedgerInfo.Builder builder =
                MLDataFormats.ManagedLedgerInfo.LedgerInfo.newBuilder();
        builder.setLedgerId(1L);

        Map<String, String> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", "value2");

        //only one copy of the offload metadata information is stored in metadata store,
        // and the original properties need to be cleared during offload
        OffloadUtils.setOffloadDriverMetadata(builder, "offload", map);

        OffloadUtils.setOffloadDriverMetadata(builder, "offload", map);

        MLDataFormats.OffloadDriverMetadata offloadDriverMetadata =
                builder.build().getOffloadContext().getDriverMetadata();
        Assert.assertEquals(offloadDriverMetadata.getPropertiesList().size(), 2);

        Assert.assertEquals(offloadDriverMetadata.getProperties(0).getKey(), "key1");
        Assert.assertEquals(offloadDriverMetadata.getProperties(1).getKey(), "key2");
        Assert.assertEquals(offloadDriverMetadata.getProperties(0).getValue(), "value1");
        Assert.assertEquals(offloadDriverMetadata.getProperties(1).getValue(), "value2");
    }

}
