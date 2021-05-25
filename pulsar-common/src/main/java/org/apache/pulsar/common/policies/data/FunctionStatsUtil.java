package org.apache.pulsar.common.policies.data;

import java.io.IOException;
import org.apache.pulsar.common.util.ObjectMapperFactory;

public class FunctionStatsUtil {

    public static FunctionStats decode (String json) throws IOException {
        return ObjectMapperFactory.getThreadLocal().readValue(json, FunctionStats.class);
    }

}
