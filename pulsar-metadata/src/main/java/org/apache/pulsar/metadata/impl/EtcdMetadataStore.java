package org.apache.pulsar.metadata.impl;

import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Watch;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

import java.time.temporal.ChronoUnit;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class EtcdMetadataStore extends AbstractMetadataStore implements MetadataStoreExtended {

    //与bookie client保持通信的组件
    private Client client;
    //存储数据用的抽象
    private KV kvClient;
    //用于节点实例在定期watch之后进行续租
    private Lease leaseClient;
    //类似于zk当中的节点状态同步
    private Watch watchClient;



    public EtcdMetadataStore(String metadataURL, MetadataStoreConfig metadataStoreConfig){
        this.client = Client.builder().endpoints(metadataURL)
                .maxInboundMessageSize(metadataStoreConfig.getMaxInboundMessageSize())
                .retryChronoUnit(ChronoUnit.MINUTES)
                .retryMaxDuration(metadataStoreConfig.getRetryMaxDuration())
                .retryDelay(metadataStoreConfig.getRetryDelay())
                .build();

        this.kvClient = client.getKVClient();
        this.leaseClient = client.getLeaseClient();


    }

    @Override
    public CompletableFuture<Optional<GetResult>> get(String path) {
        CompletableFuture<Optional<GetResult>> future = new CompletableFuture<>();
            //TODO 基于jectd实现etcd选举
            // https://www.cnblogs.com/yanh0606/p/11765691.html
//        leaseClient.grant()

        return null;
    }

    @Override
    public CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> expectedVersion) {
        return null;
    }

    @Override
    protected CompletableFuture<List<String>> getChildrenFromStore(String path) {
        return null;
    }

    @Override
    protected CompletableFuture<Boolean> existsFromStore(String path) {
        return null;
    }

    @Override
    protected CompletableFuture<Void> storeDelete(String path, Optional<Long> expectedVersion) {
        return null;
    }

    @Override
    protected CompletableFuture<Stat> storePut(String path, byte[] data, Optional<Long> optExpectedVersion, EnumSet<CreateOption> options) {
        return null;
    }
}
