/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service;

import com.google.common.annotations.VisibleForTesting;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.commons.lang3.mutable.MutableInt;

/**
 * Selects from consumers ordered by increasing priority number, with round-robin selection within a priority.
 * The caller owns the cursor and must serialize selection and membership changes using the same lock.
 * After construction, membership changes must go through this selector; sorting the list is allowed.
 * Consumer priorities must remain immutable while registered.
 *
 * <p>The type parameter lets unit tests and JMH benchmarks use lightweight consumer state to isolate
 * selection from broker setup and other {@link Consumer} behavior. Production uses {@code Consumer}
 * with {@link Consumer#getPriorityLevel()} as the priority accessor.
 *
 * @param <T> consumer representation: {@code Consumer} in production, lightweight fixtures in tests and benchmarks
 */
@NotThreadSafe
final class ConsumerPrioritySelector<T> {
    private final List<T> consumerList;
    private final ToIntFunction<T> priority;
    private final Predicate<T> available;
    private final Int2ObjectMap<MutableInt> priorityCounts = new Int2ObjectOpenHashMap<>();

    ConsumerPrioritySelector(List<T> consumerList, ToIntFunction<T> priority, Predicate<T> available) {
        this.consumerList = consumerList;
        this.priority = priority;
        this.available = available;
        consumerList.forEach(this::incrementPriorityCount);
    }

    void add(T consumer) {
        consumerList.add(consumer);
        incrementPriorityCount(consumer);
    }

    void remove(T consumer) {
        int index = consumerList.indexOf(consumer);
        if (index >= 0) {
            // Equality can match a replacement with a different priority. Count the actual removed instance.
            decrementPriorityCount(consumerList.remove(index));
        }
    }

    void removeIf(Predicate<T> predicate) {
        consumerList.removeIf(predicate);
        // Bulk removal repairs inconsistent dispatcher membership. Rebuild from survivors so even an
        // unregistered duplicate in the list cannot leave stale counts or prevent topic deletion.
        priorityCounts.clear();
        consumerList.forEach(this::incrementPriorityCount);
    }

    private void incrementPriorityCount(T consumer) {
        priorityCounts.compute(priority.applyAsInt(consumer), (level, count) -> {
            if (count == null) {
                return new MutableInt(1);
            }
            count.increment();
            return count;
        });
    }

    private void decrementPriorityCount(T consumer) {
        priorityCounts.compute(priority.applyAsInt(consumer),
                (level, count) -> count.decrementAndGet() == 0 ? null : count);
    }

    @VisibleForTesting
    int priorityLevelCount() {
        return priorityCounts.size();
    }

    // The caller supplies an in-range cursor and advances it only after a successful selection.
    int select(int cursor) {
        if (priorityCounts.size() == 1) {
            return selectSamePriority(cursor);
        }
        T current = consumerList.get(cursor);
        int targetPriority = priority.applyAsInt(current);
        int firstOnLevel = 0;

        if (targetPriority != 0) {
            // Remember where this level starts so wrapping does not rescan all higher priorities.
            for (; firstOnLevel < cursor; firstOnLevel++) {
                T consumer = consumerList.get(firstOnLevel);
                if (priority.applyAsInt(consumer) >= targetPriority) {
                    break;
                }
                if (available.test(consumer)) {
                    return firstOnLevel;
                }
            }
        }

        if (available.test(current)) {
            return cursor;
        }

        int size = consumerList.size();
        int endOfLevel = cursor + 1;
        for (; endOfLevel < size; endOfLevel++) {
            T consumer = consumerList.get(endOfLevel);
            if (priority.applyAsInt(consumer) != targetPriority) {
                break;
            }
            if (available.test(consumer)) {
                return endOfLevel;
            }
        }

        if (targetPriority == 0) {
            // Normally zero is the first level. Preserve the existing treatment of negative priorities too.
            while (priority.applyAsInt(consumerList.get(firstOnLevel)) != targetPriority) {
                firstOnLevel++;
            }
        }
        for (int i = firstOnLevel; i < cursor; i++) {
            if (available.test(consumerList.get(i))) {
                return i;
            }
        }
        for (int i = endOfLevel; i < size; i++) {
            if (available.test(consumerList.get(i))) {
                return i;
            }
        }
        return -1;
    }

    private int selectSamePriority(int cursor) {
        int size = consumerList.size();
        for (int i = cursor; i < size; i++) {
            if (available.test(consumerList.get(i))) {
                return i;
            }
        }
        for (int i = 0; i < cursor; i++) {
            if (available.test(consumerList.get(i))) {
                return i;
            }
        }
        return -1;
    }
}
