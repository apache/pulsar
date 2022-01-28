---
id: version-2.6.2-functions-runtime
title: Configure Functions runtime
sidebar_label: "Setup: Configure Functions runtime"
original_id: functions-runtime
---

Pulsar Functions support the following methods to run functions.

- *Thread*: Invoke functions in threads in Functions Worker.
- *Process*: Invoke functions in processes forked by Functions Worker.
- *Kubernetes*: Submit functions as Kubernetes StatefulSets by Functions Worker.

#### Note
> Pulsar supports adding labels to the Kubernetes StatefulSets and services while launching functions, which facilitates selecting the target Kubernetes objects.

The differences of the thread and process modes are:
- Thread mode: when a function runs in thread mode, it runs on the same Java virtual machine (JVM) with Functions worker.
- Process mode: when a function runs in process mode, it runs on the same machine that Functions worker runs.

## Configure thread runtime
It is easy to configure *Thread* runtime. In most cases, you do not need to configure anything. You can customize the thread group name with the following settings:

```yaml
functionRuntimeFactoryClassName: org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory
functionRuntimeFactoryConfigs:
  threadGroupName: "Your Function Container Group"
```

*Thread* runtime is only supported in Java function.

## Configure process runtime
When you enable *Process* runtime, you do not need to configure anything.

```yaml
functionRuntimeFactoryClassName: org.apache.pulsar.functions.runtime.process.ProcessRuntimeFactory
functionRuntimeFactoryConfigs:
  # the directory for storing the function logs
  logDirectory:
  # change the jar location only when you put the java instance jar in a different location
  javaInstanceJarLocation:
  # change the python instance location only when you put the python instance jar in a different location
  pythonInstanceLocation:
  # change the extra dependencies location:
  extraFunctionDependenciesDir:
```

*Process* runtime is supported in Java, Python, and Go functions.

## Configure Kubernetes runtime

It is easy to configure Kubernetes runtime. You can just uncomment the settings of `kubernetesContainerFactory` in the `functions_worker.yaml` file. The following is an example.

```yaml
functionRuntimeFactoryClassName: org.apache.pulsar.functions.runtime.kubernetes.KubernetesRuntimeFactory
functionRuntimeFactoryConfigs:
  # uri to kubernetes cluster, leave it to empty and it will use the kubernetes settings in function worker
  k8Uri:
  # the kubernetes namespace to run the function instances. it is `default`, if this setting is left to be empty
  jobNamespace:
  # the docker image to run function instance. by default it is `apachepulsar/pulsar`
  pulsarDockerImageName:
  # the root directory of pulsar home directory in `pulsarDockerImageName`. by default it is `/pulsar`.
  # if you are using your own built image in `pulsarDockerImageName`, you need to set this setting accordingly
  pulsarRootDir:
  # this setting only takes effects if `k8Uri` is set to null. if your function worker is running as a k8 pod,
  # setting this to true is let function worker to submit functions to the same k8s cluster as function worker
  # is running. setting this to false if your function worker is not running as a k8 pod.
  submittingInsidePod: false
  # setting the pulsar service url that pulsar function should use to connect to pulsar
  # if it is not set, it will use the pulsar service url configured in worker service
  pulsarServiceUrl:
  # setting the pulsar admin url that pulsar function should use to connect to pulsar
  # if it is not set, it will use the pulsar admin url configured in worker service
  pulsarAdminUrl:
  # the custom labels that function worker uses to select the nodes for pods
  customLabels:
  # the directory for dropping extra function dependencies
  # if it is not an absolute path, it is relative to `pulsarRootDir`
  extraFunctionDependenciesDir:
  # Additional memory padding added on top of the memory requested by the function per on a per instance basis
  percentMemoryPadding: 10
```

If you have already run a Pulsar cluster on Kubernetes, you can keep the settings unchanged at most of time.

However, if you enable RBAC on deploying your Pulsar cluster, make sure the service account you use for
running Functions Workers (or brokers, if Functions Workers run along with brokers) have permissions on the following
kubernetes APIs.

- services
- configmaps
- pods
- apps.statefulsets

Otherwise, you will not be able to create any functions. The following is an example of error message.

```bash
22:04:27.696 [Timer-0] ERROR org.apache.pulsar.functions.runtime.KubernetesRuntimeFactory - Error while trying to fetch configmap example-pulsar-4qvmb5gur3c6fc9dih0x1xn8b-function-worker-config at namespace pulsar
io.kubernetes.client.ApiException: Forbidden
	at io.kubernetes.client.ApiClient.handleResponse(ApiClient.java:882) ~[io.kubernetes-client-java-2.0.0.jar:?]
	at io.kubernetes.client.ApiClient.execute(ApiClient.java:798) ~[io.kubernetes-client-java-2.0.0.jar:?]
	at io.kubernetes.client.apis.CoreV1Api.readNamespacedConfigMapWithHttpInfo(CoreV1Api.java:23673) ~[io.kubernetes-client-java-api-2.0.0.jar:?]
	at io.kubernetes.client.apis.CoreV1Api.readNamespacedConfigMap(CoreV1Api.java:23655) ~[io.kubernetes-client-java-api-2.0.0.jar:?]
	at org.apache.pulsar.functions.runtime.KubernetesRuntimeFactory.fetchConfigMap(KubernetesRuntimeFactory.java:284) [org.apache.pulsar-pulsar-functions-runtime-2.4.0-42c3bf949.jar:2.4.0-42c3bf949]
	at org.apache.pulsar.functions.runtime.KubernetesRuntimeFactory$1.run(KubernetesRuntimeFactory.java:275) [org.apache.pulsar-pulsar-functions-runtime-2.4.0-42c3bf949.jar:2.4.0-42c3bf949]
	at java.util.TimerThread.mainLoop(Timer.java:555) [?:1.8.0_212]
	at java.util.TimerThread.run(Timer.java:505) [?:1.8.0_212]
```
If this happens, you need to grant the required permissions to the service account used for running Functions Workers. An example to grant permissions is shown below: a service account `functions-worker` is granted with permissions to access Kubernetes resources `services`, `configmaps`, `pods` and `apps.statefulsets`.

```yaml
apiVersion: rbac.authorization.k8s.io/v1beta1
kind: ClusterRole
metadata:
  name: functions-worker
rules:
- apiGroups: [""]
  resources:
  - services
  - configmaps
  - pods
  verbs:
  - '*'
- apiGroups:
  - apps
  resources:
  - statefulsets
  verbs:
  - '*'
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: functions-worker
---
apiVersion: rbac.authorization.k8s.io/v1beta1
kind: ClusterRoleBinding
metadata:
  name: functions-worker
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: functions-worker
subjects:
- kind: ServiceAccount
  name: functions-worker
```

### Kubernetes CustomRuntimeOptions

The functions (and sinks/sources) API provides a flag, `customRuntimeOptions` which can be used to pass options to the runtime to customize how the runtime operates.

In the case of case of kubernetes, this is passed to an instance of the `org.apache.pulsar.functions.runtime.kubernetes.KubernetesManifestCustomizer`. This interface can be overridden
and allows for a high degree of customization over how the K8S manifests are generated. The interface is injected by passing the class name to the `runtimeCustomizerClassName` in the `functions-worker.yaml`

To use the basic implementation, set `org.apache.pulsar.functions.runtime.kubernetes.BasicKubernetesManifestCustomizer`
for the `runtimeCustomerClassName` property. This implementation takes the following `customRuntimeOptions`
```Json
{
  "jobNamespace": "namespace", // the k8s namespace to run this function in
  "extractLabels": {           // extra labels to attach to the statefulSet, service, and pods
    "extraLabel": "value"
  },
  "extraAnnotations": {        // extra annotations to attach to the statefulSet, service, and pods
    "extraAnnotation": "value"
  },
  "nodeSelectorLabels": {      // node selector labels to add on to the pod spec
    "customLabel": "value"
  },
  "tolerations": [             // tolerations to add to the pod spec
    {
      "key": "custom-key",
      "value": "value",
      "effect": "NoSchedule"
    }
  ],
  "resourceRequirements": {  // values for cpu and memory should be defined as described here: https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container
    "requests": {
      "cpu": 1,
      "memory": "4G"
    },
    "limits": {
      "cpu": 2,
      "memory": "8G"
    }
  }
}
```
