package org.apache.pulsar.utils.misc;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.common.util.ThreadPoolMonitor;

public class ThreadPoolMonitorHelper {
    public static void registerOrderedExecutor(OrderedExecutor orderedExecutor, int threadNumber) {
        // OrderedExecutor need threadNumber * 2 to full register all thread.
        // because `OrderedExecutor.chooseThread` will i>>>1
        for (int i = 0; i < threadNumber * 2; i++) {
            ThreadPoolMonitor.register(orderedExecutor.chooseThread(i));
        }
    }

    public static void registerEventLoop(EventLoopGroup eventLoop) {
        for (EventExecutor eventExecutor : eventLoop) {
            ThreadPoolMonitor.register(eventExecutor);
        }
    }
}
