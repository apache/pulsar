package org.apache.pulsar.sql.presto.util;

import org.apache.commons.lang3.StringUtils;

public enum ReadCompactedType {

    COMPACTED_LATEST("COMPACTED_LATEST"),

    COMPACTED_EARLIEST("COMPACTED_EARLIEST");

    private final String type;

    ReadCompactedType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

//    public ReadCompactedType valueOf(String type) {
//        if (StringUtils.isEmpty(type)) {
//            return null;
//        }
//
//        for (ReadCompactedType readCompactedType : ReadCompactedType.values()) {
//            if (readCompactedType.type.equalsIgnoreCase(type)) {
//                return readCompactedType;
//            }
//        }
//        return null;
//    }

}
