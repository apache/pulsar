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
package org.apache.pulsar.testclient.utils;

import java.util.Objects;
import java.util.function.Function;

/**
 * Light-weight utility for creating rows where each column has a fixed length in a command-line setting.
 */
public class FixedColumnLengthTableMaker {
    /**
     * Character to duplicate to make the bottom border.
     */
    public char bottomBorder = '=';

    /**
     * Format String to apply to decimal entries. If set to null, no special formatting is applied.
     */
    public String decimalFormatter = null;

    /**
     * Length of table elements. Elements whose String representations exceed this length are trimmed down to this
     * length.
     */
    public int elementLength = 10;

    /**
     * The border to use to make the left side of the table.
     */
    public String leftBorder = "||";

    /**
     * The amount of spacing to pad left of an element with.
     */
    public int leftPadding = 0;

    /**
     * The border to use to make the right side of the table.
     */
    public String rightBorder = "||";

    /**
     * The amount of spacing to pad right of an element with.
     */
    public int rightPadding = 1;

    /**
     * The String to separate elements with.
     */
    public String separator = "|";

    /**
     * Character to duplicate to make the top border.
     */
    public char topBorder = '=';

    /**
     * If not null, lengthFunction should give the length for the given column index.
     */
    public Function<Integer, Integer> lengthFunction = null;

    // Helper function to add top and bottom borders.
    private void addHorizontalBorder(final int length, final StringBuilder builder, final char borderChar) {
        for (int i = 0; i < length; ++i) {
            builder.append(borderChar);
        }
    }

    // Helper function to pad with white space.
    private void addSpace(final int amount, final StringBuilder builder) {
        for (int i = 0; i < amount; ++i) {
            builder.append(' ');
        }
    }

    private int lengthFor(final int column) {
        return lengthFunction == null ? elementLength : lengthFunction.apply(column);
    }

    /**
     * Make a table using the specified settings.
     *
     * @param rows Rows to construct the table from.
     * @return A String version of the table.
     */
    public String make(final Object[][] rows) {
        final StringBuilder builder = new StringBuilder();
        int numColumns = 0;
        for (final Object[] row : rows) {
            // Take the largest number of columns out of any row to be the total.
            numColumns = Math.max(numColumns, row.length);
        }
        // Total length of the table in characters.
        int totalLength = numColumns * (leftPadding + rightPadding + separator.length()) - separator.length()
                + leftBorder.length() + rightBorder.length();
        for (int i = 0; i < numColumns; ++i) {
            totalLength += lengthFor(i);
        }
        addHorizontalBorder(totalLength, builder, topBorder);
        builder.append('\n');
        int i;
        for (final Object[] row : rows) {
            i = 0;
            builder.append(leftBorder);
            for (final Object element : row) {
                addSpace(leftPadding, builder);
                String elementString;
                if ((element instanceof Float || element instanceof Double) && decimalFormatter != null) {
                    elementString = String.format(decimalFormatter, element);
                } else {
                    // Avoid throwing NPE
                    elementString = Objects.toString(element, "");
                }
                if (elementString.length() > lengthFor(i)) {
                    // Trim down to the maximum number of characters.
                    elementString = elementString.substring(0, lengthFor(i));
                }
                builder.append(elementString);
                // Add the space due to remaining characters and the right padding.
                addSpace(lengthFor(i) - elementString.length() + rightPadding, builder);
                if (i != numColumns - 1) {
                    // Don't add separator for the last column.
                    builder.append(separator);
                }
                i += 1;
            }
            // Put empty elements for remaining columns.
            for (; i < numColumns; ++i) {
                addSpace(leftPadding + rightPadding + lengthFor(i), builder);
                if (i != numColumns - 1) {
                    builder.append(separator);
                }
            }
            builder.append(rightBorder);
            builder.append('\n');
        }
        addHorizontalBorder(totalLength, builder, bottomBorder);
        return builder.toString();
    }

}
