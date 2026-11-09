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
package org.apache.pulsar.tests.performance.launcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.testng.annotations.Test;

public class SeekSettingsTest {
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @Test
    public void readsTheSectionWithItsDefaults() throws Exception {
        assertThat(SeekSettings.read(mapper.readTree("offload: {}").path("offload").path("seeks"))).isNull();
        assertThat(SeekSettings.read(mapper.readTree("{}"))).isEqualTo(new SeekSettings(500, 1, true, true, 1800));
        assertThat(SeekSettings.read(mapper.readTree("""
                count: 2000
                seed: 7
                peek: false
                unloadFirst: false
                """))).isEqualTo(new SeekSettings(2000, 7, false, false, 1800));
    }

    @Test
    public void rejectsInvalidSettings() {
        assertThatThrownBy(() -> SeekSettings.read(mapper.readTree("count: 0")))
                .hasMessageStartingWith("offload.seeks.count must be a whole number, 1 or more");
        assertThatThrownBy(() -> SeekSettings.read(mapper.readTree("times: 5")))
                .hasMessageStartingWith("offload.seeks.times isn't a setting");
        assertThatThrownBy(() -> SeekSettings.read(mapper.readTree("peek: sometimes")))
                .hasMessageStartingWith("offload.seeks.peek must be true or false");
        assertThatThrownBy(() -> SeekSettings.read(mapper.readTree("[500]")))
                .hasMessageStartingWith("offload.seeks must be a mapping");
    }
}
