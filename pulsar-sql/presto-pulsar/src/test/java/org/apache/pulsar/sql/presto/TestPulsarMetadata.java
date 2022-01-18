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
package org.apache.pulsar.sql.presto;

import io.airlift.log.Logger;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.Test;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.core.Response;
import java.util.*;
import java.util.stream.Collectors;

import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

public class TestPulsarMetadata extends TestPulsarConnector {

    private static final Logger log = Logger.get(TestPulsarMetadata.class);

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testListSchemaNames(String delimiter) {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        List<String> schemas = this.pulsarMetadata.listSchemaNames(mock(ConnectorSession.class));


        if (StringUtils.isBlank(delimiter)) {
            String[] expectedSchemas = {NAMESPACE_NAME_1.toString(), NAMESPACE_NAME_2.toString(),
                    NAMESPACE_NAME_3.toString(), NAMESPACE_NAME_4.toString()};
            assertEquals(new HashSet<>(schemas), new HashSet<>(Arrays.asList(expectedSchemas)));
        } else {
            String[] expectedSchemas = {
                    PulsarConnectorUtils.rewriteNamespaceDelimiterIfNeeded(NAMESPACE_NAME_1.toString(), pulsarConnectorConfig),
                    PulsarConnectorUtils.rewriteNamespaceDelimiterIfNeeded(NAMESPACE_NAME_2.toString(), pulsarConnectorConfig),
                    PulsarConnectorUtils.rewriteNamespaceDelimiterIfNeeded(NAMESPACE_NAME_3.toString(), pulsarConnectorConfig),
                    PulsarConnectorUtils.rewriteNamespaceDelimiterIfNeeded(NAMESPACE_NAME_4.toString(), pulsarConnectorConfig)};
            assertEquals(new HashSet<>(schemas), new HashSet<>(Arrays.asList(expectedSchemas)));
        }
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetTableHandle(String delimiter) {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        SchemaTableName schemaTableName = new SchemaTableName(TOPIC_1.getNamespace(), TOPIC_1.getLocalName());

        ConnectorTableHandle connectorTableHandle
                = this.pulsarMetadata.getTableHandle(mock(ConnectorSession.class), schemaTableName);

        assertTrue(connectorTableHandle instanceof PulsarTableHandle);

        PulsarTableHandle pulsarTableHandle = (PulsarTableHandle) connectorTableHandle;

        assertEquals(pulsarTableHandle.getConnectorId(), pulsarConnectorId.toString());
        assertEquals(pulsarTableHandle.getSchemaName(), TOPIC_1.getNamespace());
        assertEquals(pulsarTableHandle.getTableName(), TOPIC_1.getLocalName());
        assertEquals(pulsarTableHandle.getTopicName(), TOPIC_1.getLocalName());
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetTableMetadata(String delimiter) {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        List<TopicName> allTopics = new LinkedList<>();
        allTopics.addAll(topicNames.stream().filter(topicName -> !topicName.equals(NON_SCHEMA_TOPIC)).collect(Collectors.toList()));
        allTopics.addAll(partitionedTopicNames);

        for (TopicName topic : allTopics) {
            PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                    topic.toString(),
                    topic.getNamespace(),
                    topic.getLocalName(),
                    topic.getLocalName()
            );

            List<PulsarColumnHandle> fooColumnHandles = topicsToColumnHandles.get(topic);

            ConnectorTableMetadata tableMetadata = this.pulsarMetadata.getTableMetadata(mock(ConnectorSession.class),
                    pulsarTableHandle);

            assertEquals(tableMetadata.getTable().getSchemaName(), topic.getNamespace());
            assertEquals(tableMetadata.getTable().getTableName(), topic.getLocalName());
            assertEquals(tableMetadata.getColumns().size(),
                    fooColumnHandles.size());

            List<String> fieldNames = new LinkedList<>(fooFieldNames);

            for (PulsarInternalColumn internalField : PulsarInternalColumn.getInternalFields()) {
                fieldNames.add(internalField.getName());
            }

            for (ColumnMetadata column : tableMetadata.getColumns()) {
                if (PulsarInternalColumn.getInternalFieldsMap().containsKey(column.getName())) {
                    assertEquals(column.getComment(),
                            PulsarInternalColumn.getInternalFieldsMap()
                                    .get(column.getName()).getColumnMetadata(true).getComment());
                }

                fieldNames.remove(column.getName());
            }

            assertTrue(fieldNames.isEmpty());
        }
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetTableMetadataWrongSchema(String delimiter) {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                pulsarConnectorId.toString(),
                "wrong-tenant/wrong-ns",
                TOPIC_1.getLocalName(),
                TOPIC_1.getLocalName()
        );

        try {
            ConnectorTableMetadata tableMetadata = this.pulsarMetadata.getTableMetadata(mock(ConnectorSession.class),
                    pulsarTableHandle);
            fail("Invalid schema should have generated an exception");
        } catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
            assertEquals(e.getMessage(), "Schema wrong-tenant/wrong-ns does not exist");
        }
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetTableMetadataWrongTable(String delimiter) {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                pulsarConnectorId.toString(),
                TOPIC_1.getNamespace(),
                "wrong-topic",
                "wrong-topic"
        );

        try {
            ConnectorTableMetadata tableMetadata = this.pulsarMetadata.getTableMetadata(mock(ConnectorSession.class),
                    pulsarTableHandle);
            fail("Invalid table should have generated an exception");
        } catch (TableNotFoundException e) {
            assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
            assertEquals(e.getMessage(), "Table 'tenant-1/ns-1.wrong-topic' not found");
        }
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetTableMetadataTableNoSchema(String delimiter) throws PulsarAdminException {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        ClientErrorException cee = new ClientErrorException(Response.Status.NOT_FOUND);
        when(this.schemas.getSchemaInfo(eq(TOPIC_1.getSchemaName()))).thenThrow(
                new PulsarAdminException(cee, cee.getMessage(), cee.getResponse().getStatus()));

        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                pulsarConnectorId.toString(),
                TOPIC_1.getNamespace(),
                TOPIC_1.getLocalName(),
                TOPIC_1.getLocalName()
        );


        ConnectorTableMetadata tableMetadata = this.pulsarMetadata.getTableMetadata(mock(ConnectorSession.class),
                pulsarTableHandle);
        assertEquals(tableMetadata.getColumns().size(), PulsarInternalColumn.getInternalFields().size() + 1);
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetTableMetadataTableBlankSchema(String delimiter) throws PulsarAdminException {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        SchemaInfo badSchemaInfo = SchemaInfo.builder()
                .schema(new byte[0])
                .type(SchemaType.AVRO)
                .build();
        when(this.schemas.getSchemaInfo(eq(TOPIC_1.getSchemaName()))).thenReturn(badSchemaInfo);

        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                pulsarConnectorId.toString(),
                TOPIC_1.getNamespace(),
                TOPIC_1.getLocalName(),
                TOPIC_1.getLocalName()
        );

        try {
            ConnectorTableMetadata tableMetadata = this.pulsarMetadata.getTableMetadata(mock(ConnectorSession.class),
                    pulsarTableHandle);
            fail("Table without schema should have generated an exception");
        } catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            assertEquals(e.getMessage(),
                    "Topic persistent://tenant-1/ns-1/topic-1 does not have a valid schema");
        }
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetTableMetadataTableInvalidSchema(String delimiter) throws PulsarAdminException {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        SchemaInfo badSchemaInfo = SchemaInfo.builder()
                .schema("foo".getBytes())
                .type(SchemaType.AVRO)
                .build();
        when(this.schemas.getSchemaInfo(eq(TOPIC_1.getSchemaName()))).thenReturn(badSchemaInfo);

        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                pulsarConnectorId.toString(),
                TOPIC_1.getNamespace(),
                TOPIC_1.getLocalName(),
                TOPIC_1.getLocalName()
        );

        try {
            ConnectorTableMetadata tableMetadata = this.pulsarMetadata.getTableMetadata(mock(ConnectorSession.class),
                    pulsarTableHandle);
            fail("Table without schema should have generated an exception");
        } catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            assertEquals(e.getMessage(),
                    "Topic persistent://tenant-1/ns-1/topic-1 does not have a valid schema");
        }
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testListTable(String delimiter) {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        assertTrue(this.pulsarMetadata.listTables(mock(ConnectorSession.class), Optional.empty()).isEmpty());
        assertTrue(this.pulsarMetadata.listTables(mock(ConnectorSession.class), Optional.of("wrong-tenant/wrong-ns"))
                .isEmpty());

        SchemaTableName[] expectedTopics1 = {new SchemaTableName(
            TOPIC_4.getNamespace(), TOPIC_4.getLocalName()),
            new SchemaTableName(PARTITIONED_TOPIC_4.getNamespace(), PARTITIONED_TOPIC_4.getLocalName())
        };
        assertEquals(this.pulsarMetadata.listTables(mock(ConnectorSession.class),
                Optional.of(NAMESPACE_NAME_3.toString())), Arrays.asList(expectedTopics1));

        SchemaTableName[] expectedTopics2 = {new SchemaTableName(TOPIC_5.getNamespace(), TOPIC_5.getLocalName()),
                new SchemaTableName(TOPIC_6.getNamespace(), TOPIC_6.getLocalName()),
            new SchemaTableName(PARTITIONED_TOPIC_5.getNamespace(), PARTITIONED_TOPIC_5.getLocalName()),
            new SchemaTableName(PARTITIONED_TOPIC_6.getNamespace(), PARTITIONED_TOPIC_6.getLocalName()),
        };
        assertEquals(new HashSet<>(this.pulsarMetadata.listTables(mock(ConnectorSession.class),
            Optional.of(NAMESPACE_NAME_4.toString()))), new HashSet<>(Arrays.asList(expectedTopics2)));
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetColumnHandles(String delimiter) {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(pulsarConnectorId.toString(), TOPIC_1.getNamespace(),
                TOPIC_1.getLocalName(), TOPIC_1.getLocalName());
        Map<String, ColumnHandle> columnHandleMap
                = new HashMap<>(this.pulsarMetadata.getColumnHandles(mock(ConnectorSession.class), pulsarTableHandle));

        List<String> fieldNames = new LinkedList<>(fooFieldNames);

        for (PulsarInternalColumn internalField : PulsarInternalColumn.getInternalFields()) {
            fieldNames.add(internalField.getName());
        }

        for (String field : fieldNames) {
            assertNotNull(columnHandleMap.get(field));
            PulsarColumnHandle pulsarColumnHandle = (PulsarColumnHandle) columnHandleMap.get(field);
            PulsarInternalColumn pulsarInternalColumn = PulsarInternalColumn.getInternalFieldsMap().get(field);
            if (pulsarInternalColumn != null) {
                assertEquals(pulsarColumnHandle,
                        pulsarInternalColumn.getColumnHandle(pulsarConnectorId.toString(), false));
            } else {
                assertEquals(pulsarColumnHandle.getConnectorId(), pulsarConnectorId.toString());
                assertEquals(pulsarColumnHandle.getName(), field);
                assertFalse(pulsarColumnHandle.isHidden());
            }
            columnHandleMap.remove(field);
        }
        assertTrue(columnHandleMap.isEmpty());
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testListTableColumns(String delimiter) {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        Map<SchemaTableName, List<ColumnMetadata>> tableColumnsMap
                = this.pulsarMetadata.listTableColumns(mock(ConnectorSession.class),
                new SchemaTablePrefix(TOPIC_1.getNamespace()));

        assertEquals(tableColumnsMap.size(), 4);
        List<ColumnMetadata> columnMetadataList
                = tableColumnsMap.get(new SchemaTableName(TOPIC_1.getNamespace(), TOPIC_1.getLocalName()));
        assertNotNull(columnMetadataList);
        assertEquals(columnMetadataList.size(),
                topicsToColumnHandles.get(TOPIC_1).size());

        List<String> fieldNames = new LinkedList<>(fooFieldNames);

        for (PulsarInternalColumn internalField : PulsarInternalColumn.getInternalFields()) {
            fieldNames.add(internalField.getName());
        }

        for (ColumnMetadata column : columnMetadataList) {
            if (PulsarInternalColumn.getInternalFieldsMap().containsKey(column.getName())) {
                assertEquals(column.getComment(),
                        PulsarInternalColumn.getInternalFieldsMap()
                                .get(column.getName()).getColumnMetadata(true).getComment());
            }

            fieldNames.remove(column.getName());
        }

        assertTrue(fieldNames.isEmpty());

        columnMetadataList = tableColumnsMap.get(new SchemaTableName(TOPIC_2.getNamespace(), TOPIC_2.getLocalName()));
        assertNotNull(columnMetadataList);
        assertEquals(columnMetadataList.size(),
                topicsToColumnHandles.get(TOPIC_2).size());

        fieldNames = new LinkedList<>(fooFieldNames);

        for (PulsarInternalColumn internalField : PulsarInternalColumn.getInternalFields()) {
            fieldNames.add(internalField.getName());
        }

        for (ColumnMetadata column : columnMetadataList) {
            if (PulsarInternalColumn.getInternalFieldsMap().containsKey(column.getName())) {
                assertEquals(column.getComment(),
                        PulsarInternalColumn.getInternalFieldsMap()
                                .get(column.getName()).getColumnMetadata(true).getComment());
            }

            fieldNames.remove(column.getName());
        }

        assertTrue(fieldNames.isEmpty());

        // test table and schema
        tableColumnsMap
                = this.pulsarMetadata.listTableColumns(mock(ConnectorSession.class),
                new SchemaTablePrefix(TOPIC_4.getNamespace(), TOPIC_4.getLocalName()));

        assertEquals(tableColumnsMap.size(), 1);
        columnMetadataList = tableColumnsMap.get(new SchemaTableName(TOPIC_4.getNamespace(), TOPIC_4.getLocalName()));
        assertNotNull(columnMetadataList);
        assertEquals(columnMetadataList.size(),
                topicsToColumnHandles.get(TOPIC_4).size());

        fieldNames = new LinkedList<>(fooFieldNames);

        for (PulsarInternalColumn internalField : PulsarInternalColumn.getInternalFields()) {
            fieldNames.add(internalField.getName());
        }

        for (ColumnMetadata column : columnMetadataList) {
            if (PulsarInternalColumn.getInternalFieldsMap().containsKey(column.getName())) {
                assertEquals(column.getComment(),
                        PulsarInternalColumn.getInternalFieldsMap()
                                .get(column.getName()).getColumnMetadata(true).getComment());
            }

            fieldNames.remove(column.getName());
        }

        assertTrue(fieldNames.isEmpty());
    }
}
