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
package org.apache.pulsar.functions.runtime.kubernetes;

import io.kubernetes.client.models.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.runtime.kubernetes.KubernetesManifestCustomizer;

import java.util.List;
import java.util.Map;

/**
 * An implementation of the {@link KubernetesManifestCustomizer} that allows
 * for some basic customization of namespace, labels, annotations, node selectors,
 * and tolerations.
 *
 * With the right RBAC permissions for the functions worker, these should be safe to
 * modify (for example, a service account must have permissions in the specified jobNamespace)
 *
 */
public class BasicKubernetesManifestCustomizer implements KubernetesManifestCustomizer {

    @Getter
    @Setter
    @NoArgsConstructor
    static private class RuntimeOpts {
        private String jobNamespace;
        private Map<String, String> extraLabels;
        private Map<String, String> extraAnnotations;
        private Map<String, String> nodeSelectorLabels;
        private List<V1Toleration> tolerations;
    }

    @Override
    public void initialize(Map<String, Object> config) {
    }

    @Override
    public String customizeNamespace(Function.FunctionDetails funcDetails, String currentNamespace) {
        RuntimeOpts opts = getOptsFromDetails(funcDetails);
        if (!StringUtils.isEmpty(opts.getJobNamespace())) {
            return opts.getJobNamespace();
        } else {
            return currentNamespace;
        }
    }

    @Override
    public V1Service customizeService(Function.FunctionDetails funcDetails, V1Service service) {
        RuntimeOpts opts = getOptsFromDetails(funcDetails);
        service.setMetadata(updateMeta(opts, service.getMetadata()));
        return service;
    }

    @Override
    public V1StatefulSet customizeStatefulSet(Function.FunctionDetails funcDetails, V1StatefulSet statefulSet) {
        RuntimeOpts opts = getOptsFromDetails(funcDetails);
        statefulSet.setMetadata(updateMeta(opts, statefulSet.getMetadata()));
        V1PodTemplateSpec pt = statefulSet.getSpec().getTemplate();
        pt.setMetadata(updateMeta(opts, pt.getMetadata()));
        V1PodSpec ps = pt.getSpec();
        if (opts.getNodeSelectorLabels() != null && opts.getNodeSelectorLabels().size() > 0) {
            opts.getNodeSelectorLabels().forEach(ps::putNodeSelectorItem);
        }
        if (opts.getTolerations() != null && opts.getTolerations().size() > 0) {
            opts.getTolerations().forEach(ps::addTolerationsItem);
        }
        return statefulSet;
    }

    private RuntimeOpts getOptsFromDetails(Function.FunctionDetails funcDetails) {
        String customRuntimeOptions = funcDetails.getCustomRuntimeOptions();
        RuntimeOpts opts = new Gson().fromJson(customRuntimeOptions, RuntimeOpts.class);
        // ensure that we always have at least the default
        if (opts == null) {
            opts = new RuntimeOpts();
        }
        return opts;
    }

    private V1ObjectMeta updateMeta(RuntimeOpts opts, V1ObjectMeta meta) {
        if (opts.getExtraAnnotations() != null && opts.getExtraAnnotations().size() > 0) {
            opts.getExtraAnnotations().forEach(meta::putAnnotationsItem);
        }
        if (opts.getExtraLabels() != null && opts.getExtraLabels().size() > 0) {
            opts.getExtraLabels().forEach(meta::putLabelsItem);
        }
        return meta;
    }

}
