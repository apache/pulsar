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
package org.apache.pulsar.utils.auth.tokens;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Decoders;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.Arrays;
import org.testng.annotations.Test;
import picocli.CommandLine.Option;

/**
 * TokensCliUtils Tests.
 */
public class TokensCliUtilsTest {

    @Test
    public void testCreateToken() {
        PrintStream oldStream = System.out;
        try {
            ByteArrayOutputStream baoStream = new ByteArrayOutputStream();
            System.setOut(new PrintStream(baoStream));

            new TokensCliUtils().execute(new String[]{"create-secret-key", "--base64"});
            String secretKey = baoStream.toString();

            baoStream.reset();

            String[] command = {"create", "--secret-key",
                    "data:;base64," + secretKey,
                    "--subject", "test",
                    "--headers", "kid=test",
                    "--headers", "my-k=my-v"
            };

            new TokensCliUtils().execute(command);
            String token = baoStream.toString();

            Jwt<?, ?> jwt = Jwts.parserBuilder()
                    .setSigningKey(Decoders.BASE64.decode(secretKey))
                    .build()
                    .parseClaimsJws(token);

            JwsHeader header = (JwsHeader) jwt.getHeader();
            String keyId = header.getKeyId();
            assertEquals(keyId, "test");
            assertEquals(header.get("my-k"), "my-v");

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            System.setOut(oldStream);
        }
    }

    /**
     * Test tokens generate docs.
     *
     * @throws Exception
     */
    @Test
    public void testGenerateDocs() throws Exception {
        PrintStream oldStream = System.out;
        try {
            ByteArrayOutputStream baoStream = new ByteArrayOutputStream();
            System.setOut(new PrintStream(baoStream));

            new TokensCliUtils().execute(new String[]{"gen-doc"});

            String message = baoStream.toString();

            String[] innerClassList = {
                    TokensCliUtils.CommandCreateSecretKey.class.getName(),
                    TokensCliUtils.CommandCreateKeyPair.class.getName(),
                    TokensCliUtils.CommandCreateToken.class.getName(),
                    TokensCliUtils.CommandShowToken.class.getName(),
                    TokensCliUtils.CommandValidateToken.class.getName()
            };

            for (String name : innerClassList) {
                assertInnerClass(name, message);
            }

        } finally {
            System.setOut(oldStream);
        }
    }

    private void assertInnerClass(String className, String message) throws Exception {
        Class argumentsClass = Class.forName(className);
        Field[] fields = argumentsClass.getDeclaredFields();
        for (Field field : fields) {
            boolean fieldHasAnno = field.isAnnotationPresent(Option.class);
            if (fieldHasAnno) {
                Option fieldAnno = field.getAnnotation(Option.class);
                String[] names = fieldAnno.names();
                if (names.length < 1) {
                    continue;
                }
                String nameStr = Arrays.asList(names).toString();
                nameStr = nameStr.substring(1, nameStr.length() - 1);
                assertTrue(message.indexOf(nameStr) > 0);
            }
        }
    }
}
