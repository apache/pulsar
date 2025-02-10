package org.apache.bookkeeper.mledger.util;

import io.netty.util.Recycler;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.locks.StampedLock;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.annotations.Test;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;


public class EntryWrapperPerformanceTestNG {

    private static class EntryWrapper<K, V> {
        private final Recycler.Handle<EntryWrapper> recyclerHandle;
        private static final Recycler<EntryWrapper> RECYCLER = new Recycler<EntryWrapper>() {
            @Override
            protected EntryWrapper newObject(Handle<EntryWrapper> recyclerHandle) {
                return new EntryWrapper(recyclerHandle);
            }
        };
        private final StampedLock lock = new StampedLock();
        private K key;
        private V value;
        long size;

        private EntryWrapper(Recycler.Handle<EntryWrapper> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        static <K, V> EntryWrapper<K, V> create(K key, V value, long size) {
            EntryWrapper<K, V> entryWrapper = RECYCLER.get();
            long stamp = entryWrapper.lock.writeLock();
            entryWrapper.key = key;
            entryWrapper.value = value;
            entryWrapper.size = size;
            entryWrapper.lock.unlockWrite(stamp);
            return entryWrapper;
        }

        K getKey() {
            long stamp = lock.tryOptimisticRead();
            K localKey = key;
            if (!lock.validate(stamp)) {
                stamp = lock.readLock();
                localKey = key;
                lock.unlockRead(stamp);
            }
            return localKey;
        }

        V getValue(K key) {
            long stamp = lock.tryOptimisticRead();
            K localKey = this.key;
            V localValue = this.value;
            if (!lock.validate(stamp)) {
                stamp = lock.readLock();
                localKey = this.key;
                localValue = this.value;
                lock.unlockRead(stamp);
            }
            if (localKey != key) {
                return null;
            }
            return localValue;
        }

        long getSize() {
            long stamp = lock.tryOptimisticRead();
            long localSize = size;
            if (!lock.validate(stamp)) {
                stamp = lock.readLock();
                localSize = size;
                lock.unlockRead(stamp);
            }
            return localSize;
        }

        void recycle() {
            key = null;
            value = null;
            size = 0;
            recyclerHandle.recycle(this);
        }
    }


    private static final int ITERATIONS = 1_000_000;
    private static final int THREAD_COUNT = 10; // Simulating 10 concurrent threads
    private static final int TEST_RUNS = 100; // Run each test 10 times

    private long getGCCount() {
        long count = 0;
        for (GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
            long gcCount = gcBean.getCollectionCount();
            if (gcCount != -1) {
                count += gcCount;
            }
        }
        return count;
    }

    private long getUsedMemory() {
        System.gc(); // Suggest GC before measuring memory usage
        return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    }

    private void runTest(String testName, Runnable testLogic) {
        List<Long> times = new ArrayList<>();
        List<Long> gcCounts = new ArrayList<>();
        List<Long> memoryUsages = new ArrayList<>();

        for (int i = 0; i < TEST_RUNS; i++) {
            long startGC = getGCCount();
            long startMemory = getUsedMemory();
            long startTime = System.nanoTime();

            testLogic.run();

            long elapsedTime = System.nanoTime() - startTime;
            long endGC = getGCCount();
            long endMemory = getUsedMemory();

            times.add(elapsedTime);
            gcCounts.add(endGC - startGC);
            memoryUsages.add(endMemory - startMemory);

            System.out.println(testName + " - Run " + (i + 1) + " - Time: " + TimeUnit.NANOSECONDS.toMillis(elapsedTime) + " ms, GC: " + (endGC - startGC) + ", Memory: " + (endMemory - startMemory) + " bytes");
        }

        printStatistics(testName, times, gcCounts, memoryUsages);
    }

    private void printStatistics(String testName, List<Long> times, List<Long> gcCounts, List<Long> memoryUsages) {
        System.out.println("========= " + testName + " Summary =========");
        System.out.println("Average Time: " + TimeUnit.NANOSECONDS.toMillis(average(times)) + " ms");
        System.out.println("Median Time: " + TimeUnit.NANOSECONDS.toMillis(median(times)) + " ms");
        System.out.println("P99 Time: " + TimeUnit.NANOSECONDS.toMillis(percentile(times, 99)) + " ms");
        System.out.println("Max Time: " + TimeUnit.NANOSECONDS.toMillis(Collections.max(times)) + " ms");

        System.out.println("Average GC: " + average(gcCounts));
        System.out.println("Median GC: " + median(gcCounts));
        System.out.println("P99 GC: " + percentile(gcCounts, 99));
        System.out.println("Max GC: " + Collections.max(gcCounts));

        System.out.println("Average Memory: " + average(memoryUsages) + " bytes");
        System.out.println("Median Memory: " + median(memoryUsages) + " bytes");
        System.out.println("P99 Memory: " + percentile(memoryUsages, 99) + " bytes");
        System.out.println("Max Memory: " + Collections.max(memoryUsages) + " bytes");
        System.out.println("====================================");
    }

    private long average(List<Long> values) {
        return values.stream().mapToLong(Long::longValue).sum() / values.size();
    }

    private long median(List<Long> values) {
        List<Long> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int middle = sorted.size() / 2;
        return (sorted.size() % 2 == 0) ? (sorted.get(middle - 1) + sorted.get(middle)) / 2 : sorted.get(middle);
    }

    private long percentile(List<Long> values, int percentile) {
        List<Long> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int index = (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }

    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    public static String generateString() {
        StringBuilder sb = new StringBuilder(5);
        for (int i = 0; i < 5; i++) {
            int index = ThreadLocalRandom.current().nextInt(CHARACTERS.length());
            sb.append(CHARACTERS.charAt(index));
        }
        return sb.toString();
    }

    @Test
    public void testObjectPoolingPerformance() {
        runTest("Object Pooling", () -> {
            for (int i = 0; i < ITERATIONS; i++) {
                EntryWrapper<Integer, String> entry = EntryWrapper.create(i, generateString(), i);
                entry.recycle();
            }
        });
    }

    @Test
    public void testNewObjectPerformance() {
        runTest("New Object Creation", () -> {
            for (int i = 0; i < ITERATIONS; i++) {
                var str = generateString(); // Creates a new String object each time
            }
        });
    }

    @Test
    public void testConcurrentObjectPooling() {
        runTest("Concurrent Pooling", () -> {
            ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
            List<Future<?>> futures = new ArrayList<>();

            for (int t = 0; t < THREAD_COUNT; t++) {
                futures.add(executor.submit(() -> {
                    for (int i = 0; i < ITERATIONS / THREAD_COUNT; i++) {
                        EntryWrapper<Integer, String> entry = EntryWrapper.create(i, generateString(), i);
                        entry.recycle();
                    }
                }));
            }

            futures.forEach(f -> {
                try {
                    f.get();
                } catch (Exception ignored) {}
            });

            executor.shutdown();
        });
    }

    @Test
    public void testConcurrentNewObjectCreation() {
        runTest("Concurrent New Object", () -> {
            ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
            List<Future<?>> futures = new ArrayList<>();

            for (int t = 0; t < THREAD_COUNT; t++) {
                futures.add(executor.submit(() -> {
                    //List<String> strings = new ArrayList<>();
                    for (int i = 0; i < ITERATIONS / THREAD_COUNT; i++) {
                        var str = generateString(); // Creates a new String object each time
                    }
                    //strings.clear();
                }));
            }

            futures.forEach(f -> {
                try {
                    f.get();
                } catch (Exception ignored) {}
            });

            executor.shutdown();
        });
    }
}

