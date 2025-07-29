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
package org.apache.pulsar.common.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.testng.annotations.Test;

public class StringSplitterTest {

    @Test
    public void testBasicSplitting() {
        // Test basic splitting with comma delimiter
        List<String> result = StringSplitter.splitByChar("a,b,c", ',', 0);
        assertEquals(result, Arrays.asList("a", "b", "c"));

        // Test with different delimiter
        result = StringSplitter.splitByChar("a|b|c", '|', 0);
        assertEquals(result, Arrays.asList("a", "b", "c"));

        // Test with space delimiter
        result = StringSplitter.splitByChar("hello world test", ' ', 0);
        assertEquals(result, Arrays.asList("hello", "world", "test"));
    }

    @Test
    public void testNullInput() {
        // Test null input returns empty list
        List<String> result = StringSplitter.splitByChar(null, ',', 0);
        assertEquals(result, Collections.emptyList());
        assertTrue(result.isEmpty());
    }

    @Test
    public void testEmptyString() {
        // Test empty string returns empty list
        List<String> result = StringSplitter.splitByChar("", ',', 0);
        assertEquals(result, Collections.emptyList());
        assertTrue(result.isEmpty());
    }

    @Test
    public void testSingleElement() {
        // Test string without delimiter returns single element
        List<String> result = StringSplitter.splitByChar("single", ',', 0);
        assertEquals(result, Collections.singletonList("single"));
    }

    @Test
    public void testLimitFunctionality() {
        String input = "a,b,c,d,e";

        // Test limit = 1 (should return the entire string as single element)
        List<String> result = StringSplitter.splitByChar(input, ',', 1);
        assertEquals(result, Collections.singletonList("a,b,c,d,e"));

        // Test limit = 2 (should split into 2 parts)
        result = StringSplitter.splitByChar(input, ',', 2);
        assertEquals(result, Arrays.asList("a", "b,c,d,e"));

        // Test limit = 3 (should split into 3 parts)
        result = StringSplitter.splitByChar(input, ',', 3);
        assertEquals(result, Arrays.asList("a", "b", "c,d,e"));

        // Test limit = 0 (should split all)
        result = StringSplitter.splitByChar(input, ',', 0);
        assertEquals(result, Arrays.asList("a", "b", "c", "d", "e"));

        // Test negative limit (should split all)
        result = StringSplitter.splitByChar(input, ',', -1);
        assertEquals(result, Arrays.asList("a", "b", "c", "d", "e"));
    }

    @Test
    public void testEmptyPiecesDropped() {
        // Test that empty pieces are dropped
        List<String> result = StringSplitter.splitByChar("a,,b,c", ',', 0);
        assertEquals(result, Arrays.asList("a", "b", "c"));

        // Test multiple consecutive delimiters
        result = StringSplitter.splitByChar("a,,,b,,c", ',', 0);
        assertEquals(result, Arrays.asList("a", "b", "c"));

        // Test leading delimiters are dropped
        result = StringSplitter.splitByChar(",,,a,b,c", ',', 0);
        assertEquals(result, Arrays.asList("a", "b", "c"));

        // Test trailing delimiters are dropped
        result = StringSplitter.splitByChar("a,b,c,,,", ',', 0);
        assertEquals(result, Arrays.asList("a", "b", "c"));

        // Test only delimiters
        result = StringSplitter.splitByChar(",,,", ',', 0);
        assertEquals(result, Collections.emptyList());
    }

    @Test
    public void testTrailingDelimiterHandling() {
        // Test that trailing delimiters don't create empty elements
        List<String> result = StringSplitter.splitByChar("a,b,c,", ',', 0);
        assertEquals(result, Arrays.asList("a", "b", "c"));

        // Test with limit and trailing delimiter
        result = StringSplitter.splitByChar("a,b,c,", ',', 2);
        assertEquals(result, Arrays.asList("a", "b,c,"));
    }

    @Test
    public void testSpecialCharacters() {
        // Test with special characters as content
        List<String> result = StringSplitter.splitByChar("hello@world.com,test@example.org", ',', 0);
        assertEquals(result, Arrays.asList("hello@world.com", "test@example.org"));

        // Test with numbers
        result = StringSplitter.splitByChar("123,456,789", ',', 0);
        assertEquals(result, Arrays.asList("123", "456", "789"));

        // Test with mixed content
        result = StringSplitter.splitByChar("abc123,def456,ghi789", ',', 0);
        assertEquals(result, Arrays.asList("abc123", "def456", "ghi789"));
    }

    @Test
    public void testDifferentDelimiters() {
        String input = "a:b:c";
        
        // Test with colon delimiter
        List<String> result = StringSplitter.splitByChar(input, ':', 0);
        assertEquals(result, Arrays.asList("a", "b", "c"));

        // Test with semicolon delimiter
        input = "a;b;c";
        result = StringSplitter.splitByChar(input, ';', 0);
        assertEquals(result, Arrays.asList("a", "b", "c"));

        // Test with slash delimiter
        input = "a/b/c";
        result = StringSplitter.splitByChar(input, '/', 0);
        assertEquals(result, Arrays.asList("a", "b", "c"));

        // Test with dot delimiter
        input = "a.b.c";
        result = StringSplitter.splitByChar(input, '.', 0);
        assertEquals(result, Arrays.asList("a", "b", "c"));
    }

    @Test
    public void testLargeLimit() {
        // Test with limit larger than actual splits
        String input = "a,b,c";
        List<String> result = StringSplitter.splitByChar(input, ',', 10);
        assertEquals(result, Arrays.asList("a", "b", "c"));
    }

    @Test
    public void testSingleCharacterString() {
        // Test single character that is not the delimiter
        List<String> result = StringSplitter.splitByChar("a", ',', 0);
        assertEquals(result, Collections.singletonList("a"));

        // Test single character that is the delimiter
        result = StringSplitter.splitByChar(",", ',', 0);
        assertEquals(result, Collections.emptyList());
    }

    @Test
    public void testWhitespaceHandling() {
        // Test that whitespace is preserved in elements
        List<String> result = StringSplitter.splitByChar("a b, c d ,e f", ',', 0);
        assertEquals(result, Arrays.asList("a b", " c d ", "e f"));

        // Test splitting by space with other whitespace
        result = StringSplitter.splitByChar("a\tb c\nd", ' ', 0);
        assertEquals(result, Arrays.asList("a\tb", "c\nd"));
    }

    @Test
    public void testLongStrings() {
        // Test with longer strings
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            if (i > 0) sb.append(",");
            sb.append("element").append(i);
        }
        String input = sb.toString();
        
        List<String> result = StringSplitter.splitByChar(input, ',', 0);
        assertEquals(result.size(), 100);
        assertEquals(result.get(0), "element0");
        assertEquals(result.get(99), "element99");
    }

    @Test
    public void testEdgeCasesWithLimit() {
        // Test limit with empty pieces
        List<String> result = StringSplitter.splitByChar(",,a,b,c", ',', 2);
        assertEquals(result, Arrays.asList("a", "b,c"));

        // Test limit with trailing delimiters
        result = StringSplitter.splitByChar("a,b,c,,", ',', 2);
        assertEquals(result, Arrays.asList("a", "b,c,,"));

        // Test limit = 1 with empty string
        result = StringSplitter.splitByChar("", ',', 1);
        assertEquals(result, Collections.emptyList());
    }

    @Test
    public void testUnicodeCharacters() {
        // Test with Unicode characters
        List<String> result = StringSplitter.splitByChar("α,β,γ", ',', 0);
        assertEquals(result, Arrays.asList("α", "β", "γ"));

        // Test with emoji
        result = StringSplitter.splitByChar("😀,😁,😂", ',', 0);
        assertEquals(result, Arrays.asList("😀", "😁", "😂"));
    }
}
