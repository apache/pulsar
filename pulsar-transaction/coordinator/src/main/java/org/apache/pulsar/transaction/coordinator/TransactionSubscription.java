package org.apache.pulsar.transaction.coordinator;

import com.google.common.base.Objects;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TransactionSubscription implements Comparable<TransactionSubscription> {

    private String topic;
    private String subscription;

    @Override
    public int hashCode() {
        return Objects.hashCode(topic, subscription);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransactionSubscription that = (TransactionSubscription) o;
        return topic.equals(that.topic) &&
                subscription.equals(that.subscription);
    }

    @Override
    public int compareTo(TransactionSubscription o) {
        int topicCompare = topic.compareTo(o.topic);
        if (topicCompare == 0) {
            return subscription.compareTo(o.subscription);
        }
        return topicCompare;
    }
}
