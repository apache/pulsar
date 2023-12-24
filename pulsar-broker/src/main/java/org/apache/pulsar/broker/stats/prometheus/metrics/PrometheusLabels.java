package org.apache.pulsar.broker.stats.prometheus.metrics;

import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;

public class PrometheusLabels {

    public static String backlogQuotaTypeLabel(BacklogQuotaType backlogQuotaType) {
        if (backlogQuotaType == BacklogQuotaType.message_age) {
            return "time";
        } else /* destination_storage */ {
            return "size";
        }
    }
}
