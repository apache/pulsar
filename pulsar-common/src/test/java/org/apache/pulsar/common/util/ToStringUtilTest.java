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

package org.apache.pulsar.common.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.api.proto.CursorPosition;
import org.apache.pulsar.common.api.proto.MessageRange;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class ToStringUtilTest {
    @Test
    public void testName() {
        log.info(String.format("aaa %s ", null));
    }

    @Test
    public void testPbObjectToString() {
        CursorPosition cursorPosition = new CursorPosition().setLedgerId(1).setEntryId(2);
        log.info("cursorPosition.toJson = {}", ToStringUtil.pbObjectToString(cursorPosition));
        Assert.assertEquals(ToStringUtil.pbObjectToString(cursorPosition), "{\"ledgerId\":1,\"entryId\":2}");

        cursorPosition.addProperty().setName("p").setValue(3);
        log.info("cursorPosition.toJson = {}", ToStringUtil.pbObjectToString(cursorPosition));
        Assert.assertEquals(ToStringUtil.pbObjectToString(cursorPosition),
                "{\"ledgerId\":1,\"entryId\":2,\"properties\":[{\"name\":\"p\",\"value\":3}]}");

        MessageRange ind = cursorPosition.addIndividualDeletedMessage();
        ind.setLowerEndpoint().setLedgerId(4).setEntryId(5);
        ind.setUpperEndpoint().setLedgerId(6).setEntryId(7);
        log.info("cursorPosition.toJson = {}", ToStringUtil.pbObjectToString(cursorPosition));
        Assert.assertEquals(ToStringUtil.pbObjectToString(cursorPosition), "{\"ledgerId\":1,\"entryId\":2,"
                + "\"individualDeletedMessages\":[{\"lowerEndpoint\":{\"ledgerId\":4,\"entryId\":5},"
                + "\"upperEndpoint\":{\"ledgerId\":6,\"entryId\":7}}],\"properties\":[{\"name\":\"p\",\"value\":3}]}");
    }
}