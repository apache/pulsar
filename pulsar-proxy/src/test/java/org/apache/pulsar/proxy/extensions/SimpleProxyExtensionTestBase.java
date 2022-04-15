/**
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
package org.apache.pulsar.proxy.extensions;

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.PortManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.proxy.server.ProxyConfiguration;
import org.apache.pulsar.proxy.server.ProxyService;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;

@Slf4j
@Test(groups = "proxy")
public abstract class SimpleProxyExtensionTestBase extends MockedPulsarServiceBaseTest {

    public static final class MyProxyExtension implements ProxyExtension {

        private ProxyConfiguration conf;

        @Override
        public String extensionName() {
            return "test";
        }

        @Override
        public boolean accept(String protocol) {
            return "test".equals(protocol);
        }

        @Override
        public void initialize(ProxyConfiguration conf) throws Exception {
            this.conf = conf;
        }

        @Override
        public void start(ProxyService service) {
        }

        @Override
        public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
            return Collections.singletonMap(new InetSocketAddress(conf.getBindAddress(), PortManager.nextFreePort()),
                    new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel socketChannel) throws Exception {
                    socketChannel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                        @Override
                        public void channelActive(final ChannelHandlerContext ctx) {
                            final ByteBuf resp = ctx.alloc().buffer();
                            resp.writeBytes("ok".getBytes(StandardCharsets.UTF_8));

                            final ChannelFuture f = ctx.writeAndFlush(resp);
                            f.addListener((ChannelFutureListener) future -> ctx.close());
                        }
                        @Override
                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                            log.error("error", cause);
                            ctx.close();
                        }
                    });
                }
            });
        }

        @Override
        public void close() {

        }
    }

    private File tempDirectory;
    private ProxyService proxyService;
    private boolean useSeparateThreadPoolForProxyExtensions;
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();

    public SimpleProxyExtensionTestBase(boolean useSeparateThreadPoolForProxyExtensions) {
        this.useSeparateThreadPoolForProxyExtensions = useSeparateThreadPoolForProxyExtensions;
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        tempDirectory = Files.createTempDirectory("SimpleProxyExtensionTest").toFile();

        super.internalSetup();
        proxyConfig.setUseSeparateThreadPoolForProxyExtensions(useSeparateThreadPoolForProxyExtensions);
        proxyConfig.setProxyExtensionsDirectory(tempDirectory.getAbsolutePath());
        proxyConfig.setProxyExtensions(Collections.singleton("test"));
        buildMockNarFile(tempDirectory);
        proxyConfig.setServicePort(Optional.ofNullable(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setMetadataStoreUrl(DUMMY_VALUE);
        proxyConfig.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);

        proxyService = Mockito.spy(new ProxyService(proxyConfig, new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig))));
        doReturn(new ZKMetadataStore(mockZooKeeper)).when(proxyService).createLocalMetadataStore();
        doReturn(new ZKMetadataStore(mockZooKeeperGlobal)).when(proxyService).createConfigurationMetadataStore();

        proxyService.start();
    }

    @Test
    public void testBootstrapProtocolHandler() throws Exception {
        SocketAddress address =
                proxyService.getProxyExtensions()
                      .getEndpoints()
                        .entrySet()
                        .stream()
                        .filter(e -> e.getValue().equals("test"))
                        .map(Map.Entry::getKey)
                        .findAny()
                        .get();
        try (Socket socket =  new Socket();) {
            socket.connect(address);
            String res = IOUtils.toString(socket.getInputStream(), StandardCharsets.UTF_8);
            assertEquals(res, "ok");
        }
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        proxyService.close();

        if (tempDirectory != null) {
            FileUtils.deleteDirectory(tempDirectory);
        }
    }

    private static void buildMockNarFile(File tempDirectory) throws Exception {
        File file = new File(tempDirectory, "temp.nar");
        try (ZipOutputStream zipfile = new ZipOutputStream(new FileOutputStream(file))) {

            zipfile.putNextEntry(new ZipEntry("META-INF/"));
            zipfile.putNextEntry(new ZipEntry("META-INF/services/"));
            zipfile.putNextEntry(new ZipEntry("META-INF/bundled-dependencies/"));

            ZipEntry manifest = new ZipEntry("META-INF/services/"
                    + ProxyExtensionsUtils.PROXY_EXTENSION_DEFINITION_FILE);
            zipfile.putNextEntry(manifest);
            String yaml = "name: test\n" +
                    "description: this is a test\n" +
                    "extensionClass: " + MyProxyExtension.class.getName() + "\n";
            zipfile.write(yaml.getBytes(StandardCharsets.UTF_8));
            zipfile.closeEntry();
        }
    }

}
