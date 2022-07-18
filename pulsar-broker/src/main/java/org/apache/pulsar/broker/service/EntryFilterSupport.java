package org.apache.pulsar.broker.service;

import com.google.common.collect.ImmutableList;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.EntryFilterWithClassLoader;
import org.apache.pulsar.broker.service.plugin.FilterContext;
import org.apache.pulsar.common.api.proto.MessageMetadata;

public class EntryFilterSupport {

    /**
     * Entry filters in Broker.
     * Not set to final, for the convenience of testing mock.
     */
    protected ImmutableList<EntryFilterWithClassLoader> entryFilters;
    protected final FilterContext filterContext;
    protected final Subscription subscription;

    public EntryFilterSupport(Subscription subscription) {
        this.subscription = subscription;
        if (subscription != null && subscription.getTopic() != null && MapUtils.isNotEmpty(subscription.getTopic()
                .getBrokerService().getEntryFilters())) {
            this.entryFilters = subscription.getTopic().getBrokerService().getEntryFilters().values().asList();
            this.filterContext = new FilterContext();
        } else {
            this.entryFilters = ImmutableList.of();
            this.filterContext = FilterContext.FILTER_CONTEXT_DISABLED;
        }
    }

    public EntryFilter.FilterResult runFiltersForEntry(Entry entry, MessageMetadata msgMetadata,
                                                       Consumer consumer) {
        if (CollectionUtils.isNotEmpty(entryFilters)) {
            fillContext(filterContext, msgMetadata, subscription, consumer);
            return getFilterResult(filterContext, entry, entryFilters);
        } else {
            return EntryFilter.FilterResult.ACCEPT;
        }
    }

    private void fillContext(FilterContext context, MessageMetadata msgMetadata,
                             Subscription subscription, Consumer consumer) {
        context.reset();
        context.setMsgMetadata(msgMetadata);
        context.setSubscription(subscription);
        context.setConsumer(consumer);
    }


    private static EntryFilter.FilterResult getFilterResult(FilterContext filterContext, Entry entry,
                                                            ImmutableList<EntryFilterWithClassLoader> entryFilters) {
        for (EntryFilter entryFilter : entryFilters) {
            EntryFilter.FilterResult filterResult =
                    entryFilter.filterEntry(entry, filterContext);
            if (filterResult == null) {
                filterResult = EntryFilter.FilterResult.ACCEPT;
            }
            if (filterResult != EntryFilter.FilterResult.ACCEPT) {
                return filterResult;
            }
        }
        return EntryFilter.FilterResult.ACCEPT;
    }
}
