package org.apache.pulsar.broker.service.filtering;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.regex.Pattern;

public class RegexFilter extends Filter {

    public static final String REGEX_FILTER_PATTERN_KEY = "regex_filter_pattern_key";
    private final Pattern pat;

    public RegexFilter(Properties props) {
        super(props);
        pat = Pattern.compile((String) props.get(REGEX_FILTER_PATTERN_KEY));
    }

    @Override
    public boolean matches(ByteBuf val) {
        return pat.matcher(val.readCharSequence(val.readableBytes(), StandardCharsets.UTF_8)).matches();
    }
}
