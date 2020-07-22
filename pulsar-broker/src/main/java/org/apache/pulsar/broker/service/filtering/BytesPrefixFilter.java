package org.apache.pulsar.broker.service.filtering;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class BytesPrefixFilter extends Filter {

    public static final String BYTES_PREFIX_FILTER_PREFIX = "bytes_prefix_filter_prefix";
    private final byte[] prefix;

    public BytesPrefixFilter(Properties props) {
        super(props);
        this.prefix = ((String) props.get(BYTES_PREFIX_FILTER_PREFIX)).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean matches(ByteBuf val) {
        for (int i = 0; i < prefix.length; i++) {
            if (val.readByte() != prefix[i]) {
                return false;
            }
        }
        return true;
    }
}
