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
package org.apache.pulsar.io.jdbc;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

@Slf4j
@Connector(
        name = "jdbc-postgres",
        type = IOType.SINK,
        help = "A simple JDBC sink for PostgreSQL that writes pulsar messages to a database table",
        configClass = JdbcSinkConfig.class
)
public class PostgresJdbcAutoSchemaSink extends BaseJdbcAutoSchemaSink {

    @Override
    public String generateUpsertQueryStatement() {
        final List<JdbcUtils.ColumnId> keyColumns = tableDefinition.getKeyColumns();
        if (keyColumns.isEmpty()) {
            throw new IllegalStateException("UPSERT is not supported if 'key' config is not set.");
        }
        final String keys = keyColumns.stream().map(JdbcUtils.ColumnId::getName)
                .collect(Collectors.joining(","));
        return JdbcUtils.buildInsertSql(tableDefinition)
                + " ON CONFLICT(" + keys + ") "
                + "DO UPDATE SET " + JdbcUtils.buildUpdateSqlSetPart(tableDefinition);
    }

    @Override
    public List<JdbcUtils.ColumnId> getColumnsForUpsert() {
        final List<JdbcUtils.ColumnId> columns = new ArrayList<>();
        columns.addAll(tableDefinition.getColumns());
        columns.addAll(tableDefinition.getNonKeyColumns());
        return columns;
    }

    /**
     * Handles PostgreSQL-specific array value binding with comprehensive type support.
     * <p>
     * This method converts Avro GenericData.Array objects to PostgreSQL Array objects
     * and binds them to the PreparedStatement using {@code setArray()}. It supports
     * all common PostgreSQL array types and provides detailed error handling with
     * actionable error messages.
     * </p>
     * <p>
     * <strong>Supported PostgreSQL Array Types:</strong>
     * <ul>
     * <li>INTEGER[] / INT4[] - for integer arrays</li>
     * <li>BIGINT[] / INT8[] - for long arrays</li>
     * <li>TEXT[] / VARCHAR[] / CHAR[] - for string arrays</li>
     * <li>BOOLEAN[] / BOOL[] - for boolean arrays</li>
     * <li>NUMERIC[] / DECIMAL[] - for numeric arrays</li>
     * <li>REAL[] / FLOAT4[] - for float arrays</li>
     * <li>DOUBLE PRECISION[] / FLOAT8[] - for double arrays</li>
     * <li>TIMESTAMP[] / TIMESTAMPTZ[] - for timestamp arrays</li>
     * </ul>
     * </p>
     * <p>
     * <strong>Error Handling:</strong>
     * The method provides comprehensive error handling with specific error messages for:
     * <ul>
     * <li>Null arrays (handled gracefully by setting column to NULL)</li>
     * <li>Unsupported array types (with list of supported types)</li>
     * <li>Array element type mismatches (with expected vs actual type information)</li>
     * <li>JDBC array creation failures (with database connectivity context)</li>
     * </ul>
     * </p>
     * <p>
     * <strong>Example Usage:</strong>
     * <pre>{@code
     * // Integer array binding
     * GenericData.Array<Integer> intArray = new GenericData.Array<>(schema, Arrays.asList(1, 2, 3));
     * handleArrayValue(statement, 1, intArray, "integer");
     *
     * // String array binding
     * GenericData.Array<String> textArray = new GenericData.Array<>(schema, Arrays.asList("a", "b", "c"));
     * handleArrayValue(statement, 2, textArray, "text");
     *
     * // Null array handling
     * handleArrayValue(statement, 3, null, "integer"); // Sets column to NULL
     * }</pre>
     * </p>
     *
     * @param statement     the PreparedStatement to bind the array value to
     * @param index         the parameter index (1-based) in the PreparedStatement
     * @param arrayValue    the array value to be bound, expected to be {@code GenericData.Array} or {@code Object[]}
     * @param targetSqlType the target SQL type name for the array column (e.g., "integer", "text", "_int4")
     * @throws IllegalArgumentException if the array type is unsupported or elements don't match the target type
     * @throws SQLException             if JDBC array creation or binding fails
     * @throws Exception                for other unexpected errors during array conversion
     * @see #convertToPostgresArray(Object, String, int)
     * @see #inferPostgresArrayType(String)
     * @see #validateArrayElements(Object[], String, int)
     */
    @Override
    protected void handleArrayValue(PreparedStatement statement, int index, Object arrayValue, String targetSqlType)
            throws Exception {
        // Handle null arrays using proper null handling
        if (arrayValue == null) {
            try {
                statement.setNull(index, java.sql.Types.ARRAY);
                if (log.isDebugEnabled()) {
                    log.debug("Set array column at index {} to NULL", index);
                }
            } catch (SQLException e) {
                throw new SQLException("Failed to set array column to NULL at index " + index + ": " + e.getMessage(),
                        e);
            }
            return;
        }

        // Validate input parameters
        if (targetSqlType == null || targetSqlType.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Target SQL type cannot be null or empty for array conversion at column index " + index);
        }

        try {
            Array postgresArray = convertToPostgresArray(arrayValue, targetSqlType, index);
            statement.setArray(index, postgresArray);
        } catch (IllegalArgumentException e) {
            // Re-throw validation errors with additional context
            throw new IllegalArgumentException("Array type validation failed for column at index " + index
                    + " with target type '" + targetSqlType + "': " + e.getMessage(), e);
        } catch (SQLException e) {
            // Wrap JDBC array creation failures with contextual information
            throw new SQLException("Failed to create PostgreSQL array for column at index " + index
                    + " with target type '" + targetSqlType + "'. "
                    + "Ensure the target column type supports arrays and the data types are compatible. "
                    + "Original error: " + e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
        } catch (Exception e) {
            // Wrap any other unexpected errors with context
            throw new Exception("Unexpected error during array conversion for column at index " + index
                    + " with target type '" + targetSqlType + "': " + e.getMessage(), e);
        }
    }

    /**
     * Converts an array value to a PostgreSQL Array object with comprehensive validation.
     * <p>
     * This method handles the conversion from Avro GenericData.Array or Object[] to
     * PostgreSQL-compatible arrays using the JDBC {@code Connection.createArrayOf()} method.
     * It performs type inference, element validation, and provides detailed error context
     * for troubleshooting.
     * </p>
     * <p>
     * <strong>Conversion Process:</strong>
     * <ol>
     * <li>Convert input to Object[] if it's a GenericData.Array</li>
     * <li>Infer PostgreSQL array type from targetSqlType</li>
     * <li>Validate array elements match the expected type</li>
     * <li>Create PostgreSQL Array using Connection.createArrayOf()</li>
     * </ol>
     * </p>
     * <p>
     * <strong>Supported Input Types:</strong>
     * <ul>
     * <li>{@code GenericData.Array<?>} - Avro array objects</li>
     * <li>{@code Object[]} - Java object arrays</li>
     * </ul>
     * </p>
     * <p>
     * <strong>Empty Array Handling:</strong>
     * Empty arrays are valid for any PostgreSQL array type and are handled without
     * element type validation.
     * </p>
     * <p>
     * <strong>Example Usage:</strong>
     * <pre>{@code
     * // Convert Avro array to PostgreSQL array
     * GenericData.Array<Integer> avroArray = ...;
     * Array pgArray = convertToPostgresArray(avroArray, "integer", 1);
     *
     * // Convert Object[] to PostgreSQL array
     * Object[] objectArray = {1, 2, 3};
     * Array pgArray = convertToPostgresArray(objectArray, "_int4", 2);
     * }</pre>
     * </p>
     *
     * @param arrayValue    the array value to convert ({@code GenericData.Array} or {@code Object[]})
     * @param targetSqlType the target SQL type name for the array column (e.g., "integer", "_int4")
     * @param columnIndex   the column index for error reporting and context
     * @return PostgreSQL Array object ready for binding to PreparedStatement
     * @throws SQLException             if JDBC array creation fails or database connectivity issues occur
     * @throws IllegalArgumentException if array type is unsupported, elements don't match target type,
     *                                  or input parameters are invalid
     * @see java.sql.Connection#createArrayOf(String, Object[])
     * @see #inferPostgresArrayType(String)
     * @see #validateArrayElements(Object[], String, int)
     */
    private Array convertToPostgresArray(Object arrayValue, String targetSqlType, int columnIndex) throws SQLException {
        Object[] elements;

        // Convert GenericData.Array to Object[] if needed
        if (arrayValue instanceof GenericData.Array) {
            GenericData.Array<?> avroArray = (GenericData.Array<?>) arrayValue;
            elements = avroArray.toArray();
        } else if (arrayValue instanceof Object[]) {
            elements = (Object[]) arrayValue;
        } else {
            throw new IllegalArgumentException("Unsupported array type: " + arrayValue.getClass().getName()
                    + ". Expected GenericData.Array or Object[] for column at index " + columnIndex);
        }

        // Infer PostgreSQL array type from target SQL type (this may throw IllegalArgumentException for unsupported
        // types)
        String postgresArrayType;
        try {
            postgresArrayType = inferPostgresArrayType(targetSqlType);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported array element type for column at index " + columnIndex
                    + ": " + e.getMessage());
        }

        // Handle empty arrays - they are valid for any array type
        if (elements.length == 0) {
            try {
                return getConnection().createArrayOf(postgresArrayType, elements);
            } catch (SQLException e) {
                throw new SQLException("Failed to create empty PostgreSQL array of type '" + postgresArrayType
                        + "' for column at index " + columnIndex + ": "
                        + e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
            }
        }

        // Validate array element types match expected type
        try {
            validateArrayElements(elements, postgresArrayType, columnIndex);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Array element type mismatch for column at index " + columnIndex
                    + ": " + e.getMessage());
        }

        // Create the PostgreSQL array
        try {
            return getConnection().createArrayOf(postgresArrayType, elements);
        } catch (SQLException e) {
            throw new SQLException("Failed to create PostgreSQL array of type '" + postgresArrayType
                    + "' for column at index " + columnIndex + ". "
                    + "This may indicate a data type mismatch or database connectivity issue. "
                    + "Original error: " + e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
        }
    }

    /**
     * Maps SQL type names to PostgreSQL array type strings with comprehensive type support.
     * <p>
     * This method handles the mapping from PostgreSQL column type names (as returned by
     * database metadata) to the type strings required by {@code Connection.createArrayOf()}.
     * It understands PostgreSQL's array type naming conventions and provides mappings
     * for all commonly used PostgreSQL data types.
     * </p>
     * <p>
     * <strong>PostgreSQL Array Type Conventions:</strong>
     * PostgreSQL uses underscore prefixes for array types in system catalogs:
     * <ul>
     * <li>_int4 → INTEGER[]</li>
     * <li>_text → TEXT[]</li>
     * <li>_bool → BOOLEAN[]</li>
     * </ul>
     * This method handles both prefixed and non-prefixed type names.
     * </p>
     * <p>
     * <strong>Supported Type Mappings:</strong>
     * <table border="1">
     * <tr><th>Input Type</th><th>PostgreSQL Array Type</th><th>Description</th></tr>
     * <tr><td>int4, integer</td><td>integer</td><td>32-bit integers</td></tr>
     * <tr><td>int8, bigint</td><td>bigint</td><td>64-bit integers</td></tr>
     * <tr><td>text, varchar, char</td><td>text</td><td>Variable-length strings</td></tr>
     * <tr><td>bool, boolean</td><td>boolean</td><td>Boolean values</td></tr>
     * <tr><td>numeric, decimal</td><td>numeric</td><td>Arbitrary precision numbers</td></tr>
     * <tr><td>float4, real</td><td>real</td><td>32-bit floating point</td></tr>
     * <tr><td>float8, double</td><td>float8</td><td>64-bit floating point</td></tr>
     * <tr><td>timestamp, timestamptz</td><td>timestamp</td><td>Date and time values</td></tr>
     * </table>
     * </p>
     * <p>
     * <strong>Example Usage:</strong>
     * <pre>{@code
     * // Map PostgreSQL internal type names
     * String arrayType1 = inferPostgresArrayType("_int4");     // Returns "integer"
     * String arrayType2 = inferPostgresArrayType("integer");   // Returns "integer"
     * String arrayType3 = inferPostgresArrayType("_text");     // Returns "text"
     * String arrayType4 = inferPostgresArrayType("varchar");   // Returns "text"
     * }</pre>
     * </p>
     *
     * @param targetSqlType the target SQL type name from column metadata (may include underscore prefix)
     * @return PostgreSQL array type string for use with {@code Connection.createArrayOf()}
     * @throws IllegalArgumentException if the SQL type is not supported for arrays, with detailed
     *                                  error message listing all supported types
     * @see java.sql.Connection#createArrayOf(String, Object[])
     * @see java.sql.DatabaseMetaData#getColumns(String, String, String, String)
     */
    private String inferPostgresArrayType(String targetSqlType) {
        if (targetSqlType == null || targetSqlType.trim().isEmpty()) {
            throw new IllegalArgumentException("Target SQL type cannot be null or empty for array conversion");
        }

        // Handle PostgreSQL array type naming conventions
        // PostgreSQL uses underscore prefix for array types (e.g., "_int4" for INTEGER[])
        String lowerType = targetSqlType.toLowerCase().trim();

        // Remove underscore prefix if present (array type indicators)
        if (lowerType.startsWith("_")) {
            lowerType = lowerType.substring(1);
        }

        // Map PostgreSQL internal type names to createArrayOf() type names
        switch (lowerType) {
            case "int4":
            case "integer":
                return "integer";
            case "int8":
            case "bigint":
                return "bigint";
            case "text":
            case "varchar":
            case "char":
            case "character":
            case "character varying":
                return "text";
            case "bool":
            case "boolean":
                return "boolean";
            case "numeric":
            case "decimal":
                return "numeric";
            case "float4":
            case "real":
                return "real";
            case "float8":
            case "double":
            case "double precision":
                return "float8";
            case "timestamp":
            case "timestamptz":
            case "timestamp with time zone":
            case "timestamp without time zone":
                return "timestamp";
            default:
                // Provide actionable error message with supported types
                throw new IllegalArgumentException(
                        "Unsupported PostgreSQL array element type: '" + targetSqlType + "'. "
                                + "Supported types are: INTEGER/INT4, BIGINT/INT8, TEXT/VARCHAR/CHAR, "
                                + "BOOLEAN/BOOL, NUMERIC/DECIMAL, REAL/FLOAT4, DOUBLE/FLOAT8, "
                                + "TIMESTAMP/TIMESTAMPTZ. "
                                + "Please ensure your PostgreSQL table column is defined with one of these supported "
                                + "array types.");
        }
    }

    /**
     * Coerces array elements to the expected type when possible, providing intelligent type conversion.
     * <p>
     * This method attempts to convert array elements that don't strictly match the expected PostgreSQL
     * array type but can be reasonably converted. This improves robustness when dealing with various
     * Avro schema configurations while maintaining type safety.
     * </p>
     * <p>
     * <strong>Supported Coercions:</strong>
     * <ul>
     * <li>Records to Strings: Converts Avro Records to JSON string representation</li>
     * <li>Primitive wrappers to Strings: Integer, Boolean, etc. to String</li>
     * <li>CharSequence to String: StringBuilder, StringBuffer to String</li>
     * <li>Numbers to Strings: For text array targets</li>
     * <li>String to Numbers: For numeric array targets (with validation)</li>
     * </ul>
     * </p>
     * <p>
     * <strong>Coercion Strategy:</strong>
     * <ol>
     * <li>Return null elements unchanged (always valid)</li>
     * <li>Return correctly-typed elements unchanged (fast path)</li>
     * <li>Attempt intelligent conversion for compatible types</li>
     * <li>Return original element if no safe conversion exists</li>
     * </ol>
     * </p>
     *
     * @param element           the array element to potentially coerce
     * @param postgresArrayType the target PostgreSQL array type (e.g., "text", "integer")
     * @param elementIndex      the index of the element for error reporting
     * @param columnIndex       the column index for error reporting
     * @return the coerced element, or the original element if no coercion is needed/possible
     */
    private Object coerceArrayElement(Object element, String postgresArrayType, int elementIndex, int columnIndex) {
        // Null elements are always valid
        if (element == null) {
            return null;
        }
        Class<?> elementClass = element.getClass();
        switch (postgresArrayType) {
            case "text":
                // Fast path: already a String
                if (elementClass == String.class) {
                    return element;
                }
                // Coerce various types to String
                if (element instanceof CharSequence) {
                    return element.toString();
                }
                if (element instanceof Number || element instanceof Boolean) {
                    return element.toString();
                }
                // Convert Avro Records to JSON string representation
                if (element instanceof GenericData.Record) {
                    String recordStr = element.toString();
                    if (log.isDebugEnabled()) {
                        log.debug("Coerced Avro Record to String for text array at column {} element {}: {}",
                                columnIndex, elementIndex, recordStr);
                    }
                    return recordStr;
                }
                break;
            case "integer":
                // Fast path: already an Integer
                if (elementClass == Integer.class) {
                    return element;
                }
                // Try to convert String to Integer
                if (element instanceof String) {
                    try {
                        Integer coerced = Integer.valueOf((String) element);
                        if (log.isDebugEnabled()) {
                            log.debug("Coerced String '{}' to Integer for integer array at column {} element {}",
                                    element, columnIndex, elementIndex);
                        }
                        return coerced;
                    } catch (NumberFormatException e) {
                        if (log.isDebugEnabled()) {
                            log.debug("Failed to coerce String '{}' to Integer at column {} element {}: {}",
                                    element, columnIndex, elementIndex, e.getMessage());
                        }
                    }
                }
                // Try to convert other numeric types
                if (element instanceof Number) {
                    Number num = (Number) element;
                    // Check if conversion would lose precision
                    if (num.longValue() >= Integer.MIN_VALUE && num.longValue() <= Integer.MAX_VALUE
                            && num.doubleValue() == num.longValue()) {
                        Integer coerced = num.intValue();
                        if (log.isDebugEnabled()) {
                            log.debug("Coerced {} to Integer for integer array at column {} element {}",
                                    num.getClass().getSimpleName(), columnIndex, elementIndex);
                        }
                        return coerced;
                    }
                }
                break;
            case "bigint":
                // Fast path: already a Long
                if (elementClass == Long.class) {
                    return element;
                }
                // Try to convert String to Long
                if (element instanceof String) {
                    try {
                        Long coerced = Long.valueOf((String) element);
                        if (log.isDebugEnabled()) {
                            log.debug("Coerced String '{}' to Long for bigint array at column {} element {}",
                                    element, columnIndex, elementIndex);
                        }
                        return coerced;
                    } catch (NumberFormatException e) {
                        if (log.isDebugEnabled()) {
                            log.debug("Failed to coerce String '{}' to Long at column {} element {}: {}",
                                    element, columnIndex, elementIndex, e.getMessage());
                        }
                    }
                }
                // Try to convert other numeric types
                if (element instanceof Number) {
                    Long coerced = ((Number) element).longValue();
                    if (log.isDebugEnabled()) {
                        log.debug("Coerced {} to Long for bigint array at column {} element {}",
                                element.getClass().getSimpleName(), columnIndex, elementIndex);
                    }
                    return coerced;
                }
                break;
            case "boolean":
                // Fast path: already a Boolean
                if (elementClass == Boolean.class) {
                    return element;
                }
                // Try to convert String to Boolean
                if (element instanceof String) {
                    String str = ((String) element).toLowerCase().trim();
                    if ("true".equals(str) || "1".equals(str) || "yes".equals(str)) {
                        if (log.isDebugEnabled()) {
                            log.debug("Coerced String '{}' to Boolean(true) for boolean array at column {} element {}",
                                    element, columnIndex, elementIndex);
                        }
                        return Boolean.TRUE;
                    } else if ("false".equals(str) || "0".equals(str) || "no".equals(str)) {
                        if (log.isDebugEnabled()) {
                            log.debug("Coerced String '{}' to Boolean(false) for boolean array at column {} element {}",
                                    element, columnIndex, elementIndex);
                        }
                        return Boolean.FALSE;
                    }
                }
                break;
            case "real":
                // Fast path: already a Float
                if (elementClass == Float.class) {
                    return element;
                }
                // Try to convert String to Float
                if (element instanceof String) {
                    try {
                        Float coerced = Float.valueOf((String) element);
                        if (log.isDebugEnabled()) {
                            log.debug("Coerced String '{}' to Float for real array at column {} element {}",
                                    element, columnIndex, elementIndex);
                        }
                        return coerced;
                    } catch (NumberFormatException e) {
                        if (log.isDebugEnabled()) {
                            log.debug("Failed to coerce String '{}' to Float at column {} element {}: {}",
                                    element, columnIndex, elementIndex, e.getMessage());
                        }
                    }
                }
                // Try to convert other numeric types
                if (element instanceof Number) {
                    Float coerced = ((Number) element).floatValue();
                    if (log.isDebugEnabled()) {
                        log.debug("Coerced {} to Float for real array at column {} element {}",
                                element.getClass().getSimpleName(), columnIndex, elementIndex);
                    }
                    return coerced;
                }
                break;
            case "float8":
                // Fast path: already a Double
                if (elementClass == Double.class) {
                    return element;
                }
                // Try to convert String to Double
                if (element instanceof String) {
                    try {
                        Double coerced = Double.valueOf((String) element);
                        if (log.isDebugEnabled()) {
                            log.debug("Coerced String '{}' to Double for float8 array at column {} element {}",
                                    element, columnIndex, elementIndex);
                        }
                        return coerced;
                    } catch (NumberFormatException e) {
                        if (log.isDebugEnabled()) {
                            log.debug("Failed to coerce String '{}' to Double at column {} element {}: {}",
                                    element, columnIndex, elementIndex, e.getMessage());
                        }
                    }
                }
                // Try to convert other numeric types
                if (element instanceof Number) {
                    Double coerced = ((Number) element).doubleValue();
                    if (log.isDebugEnabled()) {
                        log.debug("Coerced {} to Double for float8 array at column {} element {}",
                                element.getClass().getSimpleName(), columnIndex, elementIndex);
                    }
                    return coerced;
                }
                break;
            // For numeric, timestamp, and other types, we return the original element
            // and let the existing validation provide appropriate error messages
            default:
                break;
        }
        // Return original element if no coercion was possible or applicable
        return element;
    }

    /**
     * Validates that array elements match the expected PostgreSQL array type with detailed error reporting.
     * <p>
     * This method performs comprehensive type checking to ensure data compatibility before
     * PostgreSQL array creation. It validates both type consistency within the array and
     * compatibility with the target PostgreSQL array type. The validation helps prevent
     * runtime JDBC errors and provides actionable error messages for troubleshooting.
     * </p>
     * <p>
     * <strong>Validation Process:</strong>
     * <ol>
     * <li>Skip validation for empty arrays (always valid)</li>
     * <li>Find first non-null element as type sample</li>
     * <li>Verify all non-null elements have consistent types</li>
     * <li>Validate element type matches PostgreSQL array type expectations</li>
     * </ol>
     * </p>
     * <p>
     * <strong>Type Compatibility Rules:</strong>
     * <ul>
     * <li>integer arrays: require Integer.class elements</li>
     * <li>bigint arrays: require Long.class elements</li>
     * <li>text arrays: require String.class elements</li>
     * <li>boolean arrays: require Boolean.class elements</li>
     * <li>numeric arrays: accept Double, Float, Integer, or Long</li>
     * <li>real arrays: require Float.class elements</li>
     * <li>float8 arrays: require Double.class elements</li>
     * <li>timestamp arrays: accept Timestamp, Date, LocalDateTime, or Instant</li>
     * </ul>
     * </p>
     * <p>
     * <strong>Null Element Handling:</strong>
     * Null elements within arrays are always valid for any PostgreSQL array type.
     * Only non-null elements are validated for type consistency.
     * </p>
     * <p>
     * <strong>Error Messages:</strong>
     * The method provides detailed error messages including:
     * <ul>
     * <li>Expected vs actual Java types</li>
     * <li>Guidance on correct Avro schema types</li>
     * <li>Column index for debugging</li>
     * <li>Inconsistent type detection within arrays</li>
     * </ul>
     * </p>
     * <p>
     * <strong>Example Usage:</strong>
     * <pre>{@code
     * // Validate integer array elements
     * Object[] intElements = {1, 2, 3, null, 4};
     * validateArrayElements(intElements, "integer", 1); // Passes validation
     *
     * // This would throw IllegalArgumentException
     * Object[] mixedElements = {1, "string", 3};
     * validateArrayElements(mixedElements, "integer", 2); // Throws exception
     * }</pre>
     * </p>
     *
     * @param elements          the array elements to validate (may contain nulls)
     * @param postgresArrayType the expected PostgreSQL array type (e.g., "integer", "text")
     * @param columnIndex       the column index for error reporting and debugging context
     * @throws IllegalArgumentException if elements don't match the expected type, with detailed
     *                                  error message including expected type, actual type, and
     *                                  guidance on correct Avro schema configuration
     * @see #inferPostgresArrayType(String)
     * @see #convertToPostgresArray(Object, String, int)
     */
    private void validateArrayElements(Object[] elements, String postgresArrayType, int columnIndex) {
        if (elements.length == 0) {
            return; // Empty arrays are always valid
        }
        // First pass: Attempt to coerce elements to expected types
        boolean coercionApplied = false;
        Object[] coercedElements = elements;
        // Check if any element needs coercion
        for (int i = 0; i < elements.length; i++) {
            if (elements[i] != null) {
                Object coercedElement = coerceArrayElement(elements[i], postgresArrayType, i, columnIndex);
                if (coercedElement != elements[i]) {
                    // Create a new Object[] array to avoid type conflicts
                    if (!coercionApplied) {
                        coercedElements = new Object[elements.length];
                        System.arraycopy(elements, 0, coercedElements, 0, elements.length);
                        coercionApplied = true;
                    }
                    coercedElements[i] = coercedElement;
                }
            }
        }
        // Use the coerced elements array for the rest of the validation
        elements = coercedElements;
        if (coercionApplied && log.isDebugEnabled()) {
            log.debug("Applied type coercion to array elements for {} array at column {}",
                    postgresArrayType, columnIndex);
        }
        // Check first non-null element for type compatibility (after coercion)
        Object sampleElement = null;
        int sampleIndex = -1;
        for (int i = 0; i < elements.length; i++) {
            if (elements[i] != null) {
                sampleElement = elements[i];
                sampleIndex = i;
                break;
            }
        }
        if (sampleElement == null) {
            return; // All elements are null, which is valid for any array type
        }
        Class<?> elementClass = sampleElement.getClass();
        // Validate all non-null elements have consistent types (after coercion)
        for (int i = sampleIndex + 1; i < elements.length; i++) {
            if (elements[i] != null && !elements[i].getClass().equals(elementClass)) {
                throw new IllegalArgumentException("Inconsistent array element types after coercion: found "
                        + elementClass.getSimpleName() + " at index " + sampleIndex
                        + " and " + elements[i].getClass().getSimpleName() + " at index " + i
                        + ". Array elements could not be coerced to a consistent type for PostgreSQL "
                        + postgresArrayType + "[] column.");
            }
        }
        switch (postgresArrayType) {
            case "integer":
                if (!(elementClass == Integer.class)) {
                    throw new IllegalArgumentException("expected Integer for PostgreSQL integer[] column, got "
                            + elementClass.getSimpleName() + " after type coercion attempts. "
                            + "Original data types could not be converted to integers. "
                            + "Ensure your Avro schema uses 'int' type for integer array elements, "
                            + "or that string values contain valid integer representations.");
                }
                break;
            case "bigint":
                if (!(elementClass == Long.class)) {
                    throw new IllegalArgumentException("expected Long for PostgreSQL bigint[] column, got "
                            + elementClass.getSimpleName() + " after type coercion attempts. "
                            + "Original data types could not be converted to long integers. "
                            + "Ensure your Avro schema uses 'long' type for bigint array elements, "
                            + "or that string values contain valid long integer representations.");
                }
                break;
            case "text":
                if (!(elementClass == String.class)) {
                    throw new IllegalArgumentException("expected String for PostgreSQL text[] column, got "
                            + elementClass.getSimpleName() + " after type coercion attempts. "
                            + "Most data types should be convertible to strings, including Avro Records. "
                            + "This error suggests an unusual data type that couldn't be converted. "
                            + "Ensure your Avro schema uses 'string' type for text array elements, "
                            + "or verify that complex types like Records are properly structured.");
                }
                break;
            case "boolean":
                if (!(elementClass == Boolean.class)) {
                    throw new IllegalArgumentException("expected Boolean for PostgreSQL boolean[] column, got "
                            + elementClass.getSimpleName() + " after type coercion attempts. "
                            + "String values like 'true', 'false', '1', '0' should be convertible to booleans. "
                            + "Ensure your Avro schema uses 'boolean' type for boolean array elements, "
                            + "or that string values contain valid boolean representations.");
                }
                break;
            case "numeric":
                if (!(elementClass == Double.class || elementClass == Float.class
                        || elementClass == Integer.class || elementClass == Long.class)) {
                    throw new IllegalArgumentException(
                            "expected numeric type (Double, Float, Integer, or Long) for PostgreSQL numeric[] column"
                            + ", got " + elementClass.getSimpleName() + " after type coercion attempts. "
                            + "String representations of numbers should be convertible. "
                            + "Ensure your Avro schema uses 'double', 'float', 'int', or 'long' type for "
                            + "numeric array elements, or that string values contain valid numeric representations.");
                }
                break;
            case "real":
                if (!(elementClass == Float.class)) {
                    throw new IllegalArgumentException("expected Float for PostgreSQL real[] column, got "
                            + elementClass.getSimpleName() + " after type coercion attempts. "
                            + "String and other numeric types should be convertible to floats. "
                            + "Ensure your Avro schema uses 'float' type for real array elements, "
                            + "or that string values contain valid floating-point representations.");
                }
                break;
            case "float8":
                if (!(elementClass == Double.class)) {
                    throw new IllegalArgumentException("expected Double for PostgreSQL float8[] column, got "
                            + elementClass.getSimpleName() + " after type coercion attempts. "
                            + "String and other numeric types should be convertible to doubles. "
                            + "Ensure your Avro schema uses 'double' type for float8 array elements, "
                            + "or that string values contain valid double-precision representations.");
                }
                break;
            case "timestamp":
                if (!(elementClass == java.sql.Timestamp.class || elementClass == java.util.Date.class
                        || elementClass == java.time.LocalDateTime.class
                        || elementClass == java.time.Instant.class)) {
                    throw new IllegalArgumentException(
                            "expected timestamp type (Timestamp, Date, LocalDateTime, or Instant) for PostgreSQL "
                            + "timestamp[] column, got " + elementClass.getSimpleName()
                            + " after type coercion attempts. "
                            + "Currently, automatic conversion to timestamp types is not supported. "
                            + "Ensure your Avro schema uses appropriate timestamp type for timestamp array elements.");
                }
                break;
            default:
                // This should not happen if inferPostgresArrayType() is working correctly
                throw new IllegalArgumentException(
                        "Internal error: Unknown PostgreSQL array type for validation: " + postgresArrayType
                                + ". This indicates a bug in the array type inference logic. "
                                + "Supported types are: integer, "
                                + "bigint, text, boolean, numeric, real, float8, timestamp.");
        }
    }
}
