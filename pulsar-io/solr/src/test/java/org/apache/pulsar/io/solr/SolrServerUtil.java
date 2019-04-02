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
package org.apache.pulsar.io.solr;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;

import java.io.File;

@Slf4j
public class SolrServerUtil {
    private JettySolrRunner standaloneSolr;
    private int port;

    public SolrServerUtil(int port) {
        this.port = port;
    }

    public void startStandaloneSolr() throws Exception {
        if (standaloneSolr != null) {
            throw new IllegalStateException("Test is already running a standalone Solr instance " +
                standaloneSolr.getBaseUrl() + "! This indicates a bug in the unit test logic.");
        }

        File solrHomeDir = new File(FileUtils.getTempDirectory().getPath() + "/solr_home");
        String solrXml = "solr.xml";
        FileUtils.copyFile(getFile(solrXml), new File(solrHomeDir.getAbsolutePath() + "/" + solrXml));
        File solrLogDir = new File(solrHomeDir.getPath() + "/solr_logs");

        createTempDir(solrHomeDir);
        createTempDir(solrLogDir);

        System.setProperty("host", "localhost");
        System.setProperty("jetty.port", String.valueOf(port));
        System.setProperty("solr.log.dir", solrLogDir.getAbsolutePath());

        standaloneSolr = new JettySolrRunner(solrHomeDir.getAbsolutePath(), "/solr", port);
        Thread bg = new Thread() {
            public void run() {
                try {
                    standaloneSolr.start();
                } catch (Exception e) {
                    if (e instanceof RuntimeException) {
                        throw (RuntimeException)e;
                    } else {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
        bg.start();
    }

    public void stopStandaloneSolr() {
        if (standaloneSolr != null) {
            try {
                standaloneSolr.stop();
            } catch (Exception e) {
                log.error("Failed to stop standalone solr.");
            }
        }
    }

    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }

    private void createTempDir(File file) {
        if (!file.exists() && !file.isDirectory()) {
            file.mkdirs();
        }
    }
}
