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
package org.apache.pulsar.functions.runtime.kubernetes;

import com.google.gson.Gson;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.openapi.models.V1Toleration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.proto.Function;

/**
 * An implementation of the {@link KubernetesManifestCustomizer} that allows
 * for some basic customization of namespace, labels, annotations, node selectors,
 * and tolerations.
 *
 * With the right RBAC permissions for the functions worker, these should be safe to
 * modify (for example, a service account must have permissions in the specified jobNamespace)
 *
 */
@Slf4j
public class BasicKubernetesManifestCustomizer implements KubernetesManifestCustomizer {

    private static final String RESOURCE_CPU = "cpu";
    private static final String RESOURCE_MEMORY = "memory";
    private static final String[] RESOURCES = {RESOURCE_CPU, RESOURCE_MEMORY};

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder(toBuilder = true)
    public static class RuntimeOpts {
        private String jobNamespace;
        private String jobName;
        private Map<String, String> extraLabels;
        private Map<String, String> extraAnnotations;
        private Map<String, String> nodeSelectorLabels;
        private V1ResourceRequirements resourceRequirements;
        private List<V1Toleration> tolerations;
    }

    @Getter
    private RuntimeOpts runtimeOpts = new RuntimeOpts();

    @Override
    public void initialize(Map<String, Object> config) {
        if (config != null) {
            RuntimeOpts opts =
                    ObjectMapperFactory.getMapper().getObjectMapper().convertValue(config, RuntimeOpts.class);
            if (opts != null) {
                runtimeOpts = opts.toBuilder().build();
            }
        } else {
            log.warn("initialize with null config");
        }
    }

    @Override
    public String customizeNamespace(Function.FunctionDetails funcDetails, String currentNamespace) {
        RuntimeOpts opts = getOptsFromDetails(funcDetails);
        opts = mergeRuntimeOpts(runtimeOpts, opts);
        if (!StringUtils.isEmpty(opts.getJobNamespace())) {
            return opts.getJobNamespace();
        } else {
            return currentNamespace;
        }
    }

    @Override
    public String customizeName(Function.FunctionDetails funcDetails, String currentName) {
        RuntimeOpts opts = getOptsFromDetails(funcDetails);
        opts = mergeRuntimeOpts(runtimeOpts, opts);
        if (!StringUtils.isEmpty(opts.getJobName())) {
            return opts.getJobName();
        } else {
            return currentName;
        }
    }

    @Override
    public V1Service customizeService(Function.FunctionDetails funcDetails, V1Service service) {
        RuntimeOpts opts = getOptsFromDetails(funcDetails);
        opts = mergeRuntimeOpts(runtimeOpts, opts);
        service.setMetadata(updateMeta(opts, service.getMetadata()));
        return service;
    }

    @Override
    public V1StatefulSet customizeStatefulSet(Function.FunctionDetails funcDetails, V1StatefulSet statefulSet) {
        RuntimeOpts opts = mergeRuntimeOpts(runtimeOpts, getOptsFromDetails(funcDetails));
        statefulSet.setMetadata(updateMeta(opts, statefulSet.getMetadata()));
        V1PodTemplateSpec pt = statefulSet.getSpec().getTemplate();
        pt.setMetadata(updateMeta(opts, pt.getMetadata()));
        V1PodSpec ps = pt.getSpec();
        if (ps != null) {
            if (opts.getNodeSelectorLabels() != null && opts.getNodeSelectorLabels().size() > 0) {
                opts.getNodeSelectorLabels().forEach(ps::putNodeSelectorItem);
            }
            if (opts.getTolerations() != null && opts.getTolerations().size() > 0) {
                opts.getTolerations().forEach(ps::addTolerationsItem);
            }
            ps.getContainers().forEach(container -> updateContainerResources(container, opts));
        }
        return statefulSet;
    }

    private void updateContainerResources(V1Container container, RuntimeOpts opts) {
        if (opts.getResourceRequirements() != null) {
            V1ResourceRequirements resourceRequirements = opts.getResourceRequirements();
            V1ResourceRequirements containerResources = container.getResources();
            Map<String, Quantity> limits = resourceRequirements.getLimits();
            Map<String, Quantity> requests = resourceRequirements.getRequests();
            for (String resource : RESOURCES) {
                if (limits != null && limits.containsKey(resource)) {
                    containerResources.putLimitsItem(resource, limits.get(resource));
                }
                if (requests != null && requests.containsKey(resource)) {
                    containerResources.putRequestsItem(resource, requests.get(resource));
                }
            }
        }
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

    public static RuntimeOpts mergeRuntimeOpts(RuntimeOpts oriOpts, RuntimeOpts newOpts) {
        RuntimeOpts mergedOpts = oriOpts.toBuilder().build();
        if (mergedOpts.getExtraLabels() == null) {
            mergedOpts.setExtraLabels(new HashMap<>());
        }
        if (mergedOpts.getExtraAnnotations() == null) {
            mergedOpts.setExtraAnnotations(new HashMap<>());
        }
        if (mergedOpts.getNodeSelectorLabels() == null) {
            mergedOpts.setNodeSelectorLabels(new HashMap<>());
        }
        if (mergedOpts.getTolerations() == null) {
            mergedOpts.setTolerations(new ArrayList<>());
        }
        if (mergedOpts.getResourceRequirements() == null) {
            mergedOpts.setResourceRequirements(new V1ResourceRequirements());
        }

        if (!StringUtils.isEmpty(newOpts.getJobName())) {
            mergedOpts.setJobName(newOpts.getJobName());
        }
        if (!StringUtils.isEmpty(newOpts.getJobNamespace())) {
            mergedOpts.setJobNamespace(newOpts.getJobNamespace());
        }
        if (newOpts.getExtraLabels() != null && !newOpts.getExtraLabels().isEmpty()) {
            newOpts.getExtraLabels().forEach((key, labelsItem) -> {
                if (!mergedOpts.getExtraLabels().containsKey(key)) {
                    log.debug("extra label {} has been changed to {}", key, labelsItem);
                }
                mergedOpts.getExtraLabels().put(key, labelsItem);
            });
        }
        if (newOpts.getExtraAnnotations() != null && !newOpts.getExtraAnnotations().isEmpty()) {
            newOpts.getExtraAnnotations().forEach((key, annotationsItem) -> {
                if (!mergedOpts.getExtraAnnotations().containsKey(key)) {
                    log.debug("extra annotation {} has been changed to {}", key, annotationsItem);
                }
                mergedOpts.getExtraAnnotations().put(key, annotationsItem);
            });
        }
        if (newOpts.getNodeSelectorLabels() != null && !newOpts.getNodeSelectorLabels().isEmpty()) {
            newOpts.getNodeSelectorLabels().forEach((key, nodeSelectorItem) -> {
                if (!mergedOpts.getNodeSelectorLabels().containsKey(key)) {
                    log.debug("node selector label {} has been changed to {}", key, nodeSelectorItem);
                }
                mergedOpts.getNodeSelectorLabels().put(key, nodeSelectorItem);
            });
        }

        if (newOpts.getResourceRequirements() != null) {
            V1ResourceRequirements mergedResourcesRequirements = mergedOpts.getResourceRequirements();
            V1ResourceRequirements newResourcesRequirements = newOpts.getResourceRequirements();

            Map<String, Quantity> limits = newResourcesRequirements.getLimits();
            Map<String, Quantity> requests = newResourcesRequirements.getRequests();
            for (String resource : RESOURCES) {
                if (limits != null && limits.containsKey(resource)) {
                    mergedResourcesRequirements.putLimitsItem(resource, limits.get(resource));
                }
                if (requests != null && requests.containsKey(resource)) {
                    mergedResourcesRequirements.putRequestsItem(resource, requests.get(resource));
                }
            }
            mergedOpts.setResourceRequirements(mergedResourcesRequirements);
        }

        if (newOpts.getTolerations() != null && !newOpts.getTolerations().isEmpty()) {
            mergedOpts.getTolerations().addAll(newOpts.getTolerations());
        }
        return mergedOpts;
    }

}
