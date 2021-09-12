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

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * SolrSinkConfig test
 */
public class SolrSinkConfigTest {

    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File yamlFile = getFile("sinkConfig.yaml");
        String path = yamlFile.getAbsolutePath();
        SolrSinkConfig config = SolrSinkConfig.load(path);
        assertNotNull(config);
        assertEquals(config.getSolrUrl(), "localhost:2181,localhost:2182/chroot");
        assertEquals(config.getSolrMode(), "SolrCloud");
        assertEquals(config.getSolrCollection(), "techproducts");
        assertEquals(config.getSolrCommitWithinMs(), Integer.parseInt("100"));
        assertEquals(config.getUsername(), "fakeuser");
        assertEquals(config.getPassword(), "fake@123");
    }

    @Test
    public final void loadFromMapTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("solrUrl", "localhost:2181,localhost:2182/chroot");
        map.put("solrMode", "SolrCloud");
        map.put("solrCollection", "techproducts");
        map.put("solrCommitWithinMs", "100");
        map.put("username", "fakeuser");
        map.put("password", "fake@123");

        SolrSinkConfig config = SolrSinkConfig.load(map);
        assertNotNull(config);
        assertEquals(config.getSolrUrl(), "localhost:2181,localhost:2182/chroot");
        assertEquals(config.getSolrMode(), "SolrCloud");
        assertEquals(config.getSolrCollection(), "techproducts");
        assertEquals(config.getSolrCommitWithinMs(), Integer.parseInt("100"));
        assertEquals(config.getUsername(), "fakeuser");
        assertEquals(config.getPassword(), "fake@123");
    }

    @Test
    public final void validValidateTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("solrUrl", "localhost:2181,localhost:2182/chroot");
        map.put("solrMode", "SolrCloud");
        map.put("solrCollection", "techproducts");
        map.put("solrCommitWithinMs", "100");
        map.put("username", "fakeuser");
        map.put("password", "fake@123");

        SolrSinkConfig config = SolrSinkConfig.load(map);
        config.validate();
    }

    @Test(expectedExceptions = NullPointerException.class,
        expectedExceptionsMessageRegExp = "solrUrl property not set.")
    public final void missingValidValidateSolrModeTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("solrMode", "SolrCloud");
        map.put("solrCollection", "techproducts");
        map.put("solrCommitWithinMs", "100");
        map.put("username", "fakeuser");
        map.put("password", "fake@123");

        SolrSinkConfig config = SolrSinkConfig.load(map);
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "solrCommitWithinMs must be a positive integer.")
    public final void invalidBatchTimeMsTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("solrUrl", "localhost:2181,localhost:2182/chroot");
        map.put("solrMode", "SolrCloud");
        map.put("solrCollection", "techproducts");
        map.put("solrCommitWithinMs", "-100");
        map.put("username", "fakeuser");
        map.put("password", "fake@123");

        SolrSinkConfig config = SolrSinkConfig.load(map);
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "No enum constant org.apache.pulsar.io.solr.SolrAbstractSink.SolrMode.NOTSUPPORT")
    public final void invalidClientModeTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("solrUrl", "localhost:2181,localhost:2182/chroot");
        map.put("solrMode", "NotSupport");
        map.put("solrCollection", "techproducts");
        map.put("solrCommitWithinMs", "100");
        map.put("username", "fakeuser");
        map.put("password", "fake@123");

        SolrSinkConfig config = SolrSinkConfig.load(map);
        config.validate();

        SolrAbstractSink.SolrMode.valueOf(config.getSolrMode().toUpperCase());
    }

    @Test
    public final void validZkChrootTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("solrUrl", "localhost:2181,localhost:2182/chroot");
        map.put("solrMode", "SolrCloud");
        map.put("solrCollection", "techproducts");
        map.put("solrCommitWithinMs", "100");
        map.put("username", "fakeuser");
        map.put("password", "fake@123");

        SolrSinkConfig config = SolrSinkConfig.load(map);
        config.validate();

        String url = config.getSolrUrl();
        int chrootIndex = url.indexOf("/");
        Optional<String> chroot = Optional.empty();
        if (chrootIndex > 0) {
            chroot = Optional.of(url.substring(chrootIndex));
        }
        String zkUrls = chrootIndex > 0 ? url.substring(0, chrootIndex) : url;
        List<String> zkHosts = Arrays.asList(zkUrls.split(","));

        List<String> expectedZkHosts = Lists.newArrayList();
        expectedZkHosts.add("localhost:2181");
        expectedZkHosts.add("localhost:2182");

        assertEquals("/chroot", chroot.get());
        assertEquals(expectedZkHosts, zkHosts);
    }

    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }
}
