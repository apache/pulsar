package org.apache.pulsar.io;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;

import java.util.Map;

public class PushSourceWithSecrets<T> extends PushSource<T> {

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {

    }

    @Override
    public void close() throws Exception {

    }
}
