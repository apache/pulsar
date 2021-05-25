package org.apache.pulsar.common.policies.data;

import org.apache.pulsar.common.util.ObjectMapperFactory;
import java.io.IOException;

public class FunctionStatusUtil {
    public static FunctionStatus decode(String json) throws IOException {
        return ObjectMapperFactory.getThreadLocal().readValue(json, FunctionStatus.class);
    }
}
