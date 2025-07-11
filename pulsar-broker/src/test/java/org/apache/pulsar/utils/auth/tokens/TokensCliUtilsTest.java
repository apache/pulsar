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

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Decoders;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import picocli.CommandLine.Option;

/**
 * TokensCliUtils Tests.
 */
public class TokensCliUtilsTest {

    @DataProvider(name = "desiredExpireTime")
    public Object[][] desiredExpireTime() {
        return new Object[][] {
                {"600", 600}, //10m
                {"5m", 300},
                {"1h", 3600},
                {"1d", 86400},
                {"1w", 604800},
                {"1y", 31536000}
        };
    }

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

    @Test(dataProvider = "desiredExpireTime")
    public void commandCreateToken_WhenCreatingATokenWithExpiryTime_ShouldHaveTheDesiredExpireTime(String expireTime, int expireAsSec) throws Exception {
        PrintStream oldStream = System.out;
        try {
            //Arrange
            ByteArrayOutputStream baoStream = new ByteArrayOutputStream();
            System.setOut(new PrintStream(baoStream));

            String[] command = {"create", "--secret-key",
                    "data:;base64,u+FxaxYWpsTfxeEmMh8fQeS3g2jfXw4+sGIv+PTY+BY=",
                    "--subject", "test",
                    "--expiry-time", expireTime,
            };

            new TokensCliUtils().execute(command);
            String token = baoStream.toString();

            Instant start = (new Date().toInstant().plus(expireAsSec - 5, ChronoUnit.SECONDS));
            Instant stop = (new Date().toInstant().plus(expireAsSec + 5, ChronoUnit.SECONDS));

            //Act
            Claims jwt = Jwts.parserBuilder()
                    .setSigningKey(Decoders.BASE64.decode("u+FxaxYWpsTfxeEmMh8fQeS3g2jfXw4+sGIv+PTY+BY="))
                    .build()
                    .parseClaimsJws(token)
                    .getBody();

            //Assert
            //Checks if the token expires within +-5 sec.
            assertTrue(( ! jwt.getExpiration().toInstant().isBefore( start ) ) && ( jwt.getExpiration().toInstant().isBefore( stop ) ));

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
