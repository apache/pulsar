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
package org.apache.pulsar.broker.authentication.scram;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.common.sasl.scram.ScramFormatter;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.naming.directory.AttributeModificationException;
import java.security.InvalidKeyException;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class SaslScramDataTest {

    @BeforeMethod
    public void setUp() {
    }

    @AfterMethod
    public void tearDown() {
    }

    @Test
    public void testScramData() {

        ObjectMapper objectMapper = new ObjectMapper();

        ZookeeperSaslScramUser user = null;
        String scramData = null;
        try {

            String password = "abc";

            scramData = ScramFormatter.credentialToString(new ScramFormatter().generateCredential(password, 10000));

            String userData = "{\"SCRAM_SHA_256\":\"" + scramData + "\",\"password\":\"" + password + "\"}";

            System.out.println(userData);

            user = objectMapper.readValue(userData, ZookeeperSaslScramUser.class);

            assertTrue(scramData.equals(user.getScramSha256()));
            assertTrue(password.equals(user.getPassword()));

        } catch (JsonProcessingException e) {
            fail("parse data failed");
        } catch (InvalidKeyException e) {
            fail("parse data failed");
        } catch (AttributeModificationException e) {
            fail("parse data failed");
        }
    }
}