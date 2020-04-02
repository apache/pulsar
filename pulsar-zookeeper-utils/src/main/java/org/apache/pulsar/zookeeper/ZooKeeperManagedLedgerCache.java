package org.apache.pulsar.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class ZooKeeperManagedLedgerCache implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperManagedLedgerCache.class);

    private final ZooKeeperCache cache;
    private final String path;

    public ZooKeeperManagedLedgerCache(ZooKeeperCache cache, String path) {
        this.cache = cache;
        this.path = path;
    }

    public Set<String> get(String path) throws KeeperException, InterruptedException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getChildren called at: {}", path);
        }

        Set<String> children = cache.getChildrenAsync(path, this).join();
        if (children == null) {
            throw KeeperException.create(KeeperException.Code.NONODE);
        }

        return children;
    }

    public CompletableFuture<Set<String>> getAsync(String path) {
        return cache.getChildrenAsync(path, this);
    }

    public void clearTree() {
        cache.invalidateRoot(path);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        LOG.info("[{}] Received ZooKeeper watch event: {}", cache.zkSession.get(), watchedEvent);
        String watchedEventPath = watchedEvent.getPath();
        if (watchedEventPath != null) {
            LOG.info("invalidate called in zookeeperChildrenCache for path {}", watchedEventPath);
            cache.invalidate(path);
        }
    }
}
