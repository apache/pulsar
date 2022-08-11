package org.apache.pulsar.proxy.server;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.AutoCloseUselessClientConTXTest;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.proxy.util.ProxyServiceFactory;
import org.apache.pulsar.proxy.util.ProxyServiceInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class AutoCloseUselessClientConTXProxyTest extends AutoCloseUselessClientConTXTest {

    private ProxyServiceInfo proxyServiceInfo;

    @Override
    @BeforeMethod
    public void before() throws Exception {
        super.before();
        proxyServiceInfo = ProxyServiceFactory.startProxyService(mockZooKeeper, mockZooKeeperGlobal, alreadyUsedPort());
    }

    @Override
    protected PulsarClientImpl choosePulsarClient() throws Exception {
        PulsarClientImpl pulsarClient = (PulsarClientImpl) PulsarClient.builder()
                .operationTimeout(30000, TimeUnit.SECONDS)
                .serviceUrl("pulsar://" + proxyServiceInfo.getProxyAddress().getHostString() + ":"
                        + proxyServiceInfo.getProxyAddress().getPort())
                .enableTransaction(true)
                .build();
        return pulsarClient;
    }

    @AfterMethod
    public void afterMethod() throws Exception{
        proxyServiceInfo.getProxyService().close();
    }

    @Override
    protected void connectionToEveryBrokerWithUnloadBundle(PulsarClientImpl pulsarClient){
        connectionToEveryBroker(pulsarClient, proxyServiceInfo.getProxyAddress());
    }

    @Override
    protected void connectionToEveryBroker(PulsarClientImpl pulsarClient, InetSocketAddress proxyAddress){
        super.connectionToEveryBroker(pulsarClient, proxyServiceInfo.getProxyAddress());
    }
}
