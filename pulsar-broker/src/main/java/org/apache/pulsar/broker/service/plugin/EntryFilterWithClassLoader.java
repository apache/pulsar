package org.apache.pulsar.broker.service.plugin;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.common.nar.NarClassLoader;

public class EntryFilterWithClassLoader implements EntryFilter {
    private final EntryFilter entryFilter;
    private final NarClassLoader classLoader;

    public EntryFilterWithClassLoader(EntryFilter entryFilter, NarClassLoader classLoader) {
        this.entryFilter = entryFilter;
        this.classLoader = classLoader;
    }

    @Override
    public FilterResult filterEntry(Entry entry, FilterContext context) {
        return entryFilter.filterEntry(entry, context);
    }
}
