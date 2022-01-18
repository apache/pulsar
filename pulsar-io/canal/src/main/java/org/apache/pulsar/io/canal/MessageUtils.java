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
package org.apache.pulsar.io.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A Simple class for mysql binlog message to parse.
 */
public class MessageUtils {

    public static Map<String, String> genColumn(CanalEntry.Column column) {
        Map<String, String> row = new LinkedHashMap<>();
        if (column.getIsKey()) {
            row.put("isKey", "1");
        } else {
            row.put("isKey", "0");
        }
        if (column.getIsNull()) {
            row.put("isNull", "1");
        } else {
            row.put("isNull", "0");
        }
        row.put("index", Integer.toString(column.getIndex()));
        row.put("mysqlType", column.getMysqlType());
        row.put("columnName", column.getName());
        if (column.getIsNull()) {
            row.put("columnValue", null);
        } else {
            row.put("columnValue", column.getValue());
        }
        return row;
    }

    /**
     * Message convert to FlatMessage.
     *
     * @param message
     * @return FlatMessage List
     */
    public static List<FlatMessage> messageConverter(Message message) {
        try {
            if (message == null) {
                return null;
            }

            List<FlatMessage> flatMessages = new ArrayList<>();
            List<CanalEntry.Entry> entrys = null;
            if (message.isRaw()) {
                List<ByteString> rawEntries = message.getRawEntries();
                entrys = new ArrayList<CanalEntry.Entry>(rawEntries.size());
                for (ByteString byteString : rawEntries) {
                    CanalEntry.Entry entry = CanalEntry.Entry.parseFrom(byteString);
                    entrys.add(entry);
                }
            } else {
                entrys = message.getEntries();
            }

            for (CanalEntry.Entry entry : entrys) {
                if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                        || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                    continue;
                }

                CanalEntry.RowChange rowChange;
                try {
                    rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("ERROR ## parser of eromanga-event has an error, data:"
                            + entry.toString(), e);
                }

                CanalEntry.EventType eventType = rowChange.getEventType();

                FlatMessage flatMessage = new FlatMessage(message.getId());
                flatMessages.add(flatMessage);
                flatMessage.setDatabase(entry.getHeader().getSchemaName());
                flatMessage.setTable(entry.getHeader().getTableName());
                flatMessage.setIsDdl(rowChange.getIsDdl());
                flatMessage.setType(eventType.toString());
                flatMessage.setEs(entry.getHeader().getExecuteTime());
                flatMessage.setTs(System.currentTimeMillis());
                flatMessage.setSql(rowChange.getSql());

                if (!rowChange.getIsDdl()) {
                    List<Map<String, String>> data = new ArrayList<>();
                    List<Map<String, String>> old = new ArrayList<>();

                    for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                        if (eventType != CanalEntry.EventType.INSERT && eventType != CanalEntry.EventType.UPDATE
                                && eventType != CanalEntry.EventType.DELETE) {
                            continue;
                        }

                        List<CanalEntry.Column> columns;

                        if (eventType == CanalEntry.EventType.DELETE) {
                            columns = rowData.getBeforeColumnsList();
                        } else {
                            columns = rowData.getAfterColumnsList();
                        }
                        columns.size();
                        for (CanalEntry.Column column : columns) {
                            Map<String, String> row = genColumn(column);
                            if (column.getUpdated()) {
                                row.put("updated", "1");
                            } else {
                                row.put("updated", "0");
                            }
                            data.add(row);
                        }

                        if (eventType == CanalEntry.EventType.UPDATE) {
                            for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                                Map<String, String> rowOld = genColumn(column);
                                old.add(rowOld);
                            }
                        }
                    }

                    if (!data.isEmpty()) {
                        flatMessage.setData(data);
                    }
                    if (!old.isEmpty()) {
                        flatMessage.setOld(old);
                    }
                }
            }
            return flatMessages;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
