---
id: functions-runtime
title: Configure Functions runtime
sidebar_label: "Setup: Configure Functions runtime"
---

Pulsar Functions support the following methods to run functions.

- *Thread*: Invoke functions threads in functions worker.
- *Process*: Invoke functions in processes forked by functions worker.
- *Kubernetes*: Submit functions as Kubernetes StatefulSets by functions worker.

#### Note
> Pulsar supports adding labels to the Kubernetes StatefulSets and services while launching functions, which facilitates selecting the target Kubernetes objects.

The differences of the thread and process modes are:
- Thread mode: when a function runs in thread mode, it runs on the same Java virtual machine (JVM) with functions worker.
- Process mode: when a function runs in process mode, it runs on the same machine that functions worker runs.

## Configure thread runtime
It is easy to configure *Thread* runtime. In most cases, you do not need to configure anything. You can customize the thread group name with the following settings:

```yaml
threadContainerFactory:
  threadGroupName: "Your Function Container Group"
```

*Thread* runtime is only supported in Java function.

## Configure process runtime
When you enable *Process* runtime, you do not need to configure anything.

```yaml
processContainerFactory:
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


### How it works

The Kubernetes runtime works by having the functions worker generate and apply Kubernetes manifests. In the event that the functions worker is running on Kubernetes already, it can use the `serviceAccount` that is associated with the pod the functions worker is running in. Otherwise, it can be configured to communicate with a Kubernetes cluster.

The manifests which the functions worker generates include a `StatefulSet`, a `Service` (which is used to communicate with the pods), and a `Secret` for auth credentials (when applicable). The `StatefulSet` manifest (by default) has a single pod, with the number of replicas determined by the "parallelism" of the function. On pod boot, the pod downloads the function payload (via the functions worker REST API). The pod's container image is configurable, but must have the functions runtime.

The Kubernetes runtime also supports secrets, with the end user being able to create a Kubernetes secret and have it be exposed as an environment variable in the pod (described below). Additionally, the Kubernetes runtime fairly extensible, with the user being able to implement classes that customize the way Kubernetes manifests get generated, how auth data is passed to pods, and how secrets can be integrated.

### Basic configuration

It is easy to configure Kubernetes runtime. You can just uncomment the settings of `kubernetesContainerFactory` in the `functions_worker.yaml` file. The following is an example.

```yaml
kubernetesContainerFactory:
  # uri to kubernetes cluster, leave it to empty and it will use the kubernetes settings in function worker
  k8Uri:
  # the kubernetes namespace to run the function instances. it is `default`, if this setting is left to be empty
  jobNamespace:
  # the docker image to run function instance. by default it is `apachepulsar/pulsar`
  pulsarDockerImageName:
  # the docker image to run function instance according to different configurations provided by users.
  # By default it is `apachepulsar/pulsar`.
  # e.g:
  # functionDockerImages:
  #   JAVA: JAVA_IMAGE_NAME
  #   PYTHON: PYTHON_IMAGE_NAME
  #   GO: GO_IMAGE_NAME
  functionDockerImages:
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

As stated earlier, if you already run your functions worker embedded in a broker on Kubernetes, you can keep many of these settings as default.

### Standalone functions worker on K8S

If you run your functions worker standalone (that is, not embedded) on Kubernetes, you need to configure `pulsarSerivceUrl` to be the URL of the
broker and `pulsarAdminUrl` as the URL to the functions worker.

As an example, suppose both our Pulsar brokers and Function Workers run in the `pulsar` K8S namespace. Additionally, assuming the brokers have a service called `brokers` and the functions worker has a service called `func-worker`, then the settings would be:

```yaml
pulsarServiceUrl: pulsar://broker.pulsar:6650 // or pulsar+ssl://broker.pulsar:6651 if using TLS
pulsarAdminUrl: http://func-worker.pulsar:8080 // or https://func-worker:8443 if using TLS
```

### Kubernetes RBAC

If you are running RBAC in your Kubernetes cluster, make sure the service account you use for
running functions workers (or brokers, if functions workers run along with brokers) have permissions on the following
kubernetes APIs.

- services
- configmaps
- pods
- apps.statefulsets

The following should be sufficient:

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
subjectsKubernetesSec:
- kind: ServiceAccount
  name: functions-worker
```

If the service-account is not properly configured, you may see an error message similar to this:
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

### Kubernetes Secrets integration

In order to safely distribute secrets, Pulasr Functions can reference Kubernetes secrets. To enable this, set the `secretsProviderConfiguratorClassName` to `org.apache.pulsar.functions.secretsproviderconfigurator.KubernetesSecretsProviderConfigurator`.

Then, you can create a secret in the namespace where your functions are deployed. As an example, suppose we are deploying functions to the `pulsar-func` Kubernetes namespace, and we have a secret named `database-creds` with a field name `password`, which we want to mount in the pod as an environment variable called `DATABASE_PASSWORD`.

The following functions configuration would then allow us to reference that secret and have the value be mounted as an environment variable in the pod.

```Yaml
tenant: "mytenant"
namespace: "mynamespace"
name: "myfunction"
topicName: "persistent://mytenant/mynamespace/myfuncinput"
className: "com.company.pulsar.myfunction"

secrets:
  # the secret will be mounted from the `password` field in the `database-creds` secret as an env var called `DATABASE_PASSWORD`
  DATABASE_PASSWORD:
    path: "database-creds"
    key: "password"

```

### Kubernetes Functions authentication


When your Pulsar cluster uses authentication, the pod running your function needs a mechanism to authenticate with the broker.

An interface, `org.apache.pulsar.functions.auth.KubernetesFunctionAuthProvider`, can be extended to provide support for any authentication mechanism. The `functionAuthProviderClassName` in `function-worker.yml` is used to specify your path to this implementation.

Pulsar includes an implementation of this interface that is suitable for token auth, which should be configured like the following in the configuration:
```Yaml
functionAuthProviderClassName: org.apache.pulsar.functions.auth.KubernetesSecretsTokenAuthProvider
```

For custom authentication or TLS, you need to implement this interface (or use an alternative mechanism to provide authentication)

For token authentication, the way this works is that the functions worker captures the token that was used to deploy (or update) the function. This token is then saved as a secret and mounted into the pod.

One thing to keep in mind is that if you use tokens that expire when deploying functions, these tokens will expire.


### Kubernetes CustomRuntimeOptions

The Kubernetes integration also includes the ability for the user to implement a class which customizes how manifests is generated. This is configured by setting the `runtimeCustomizerClassName` in the `functions-worker.yml` to the fully qualified class name. The interface the you must implement is the `org.apache.pulsar.functions.runtime.kubernetes.KubernetesManifestCustomizer` interface.

The functions (and sinks/sources) API provides a flag, `customRuntimeOptions`, which is passed to this interface.

Pulsar also includes a built-in implementation. To use this basic implementation, set `runtimeCustomizerClassName` to `org.apache.pulsar.functions.runtime.kubernetes.BasicKubernetesManifestCustomizer`. This built-in implementation allows you pass a JSON document with certain properties to augment how the manifests are generated, for example:

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

## Other configuration considerations


### In a cluster with geo-replication

If you are running multiple clusters tied together with geo-replication, it is important to use a different function namespace for each cluster. Otherwise, the function
shares a namespace and potentially schedule across clusters.

As an example, suppose we have clusters east-1 and west-1, we would configure the functions worker in east-1 like:
```Yaml
pulsarFunctionsCluster: east-1
pulsarFunctionsNamespace: public/functions-east-1
```

and the cluster in west-1 like:
```Yaml
pulsarFunctionsCluster: west-1
pulsarFunctionsNamespace: public/functions-west-1
```

This ensures the two different Function Workers use distinct sets of topics for their internal coordination.

### Configure standalone functions worker

When configuring a standalone functions worker, you need to specify a few properties in order for the functions worker to be able
to communicate with the broker. This requires many of the same properties to be set that the broker requires, especially when using TLS.

The following properties are the baseline of what is required:

```Yaml
workerPort: 8080
workerPortTls: 8443 # when using TLS
tlsCertificateFilePath: /etc/pulsar/tls/tls.crt # when using TLS
tlsKeyFilePath: /etc/pulsar/tls/tls.key # when using TLS
tlsTrustCertsFilePath: /etc/pulsar/tls/ca.crt # when using TLS
pulsarServiceUrl: pulsar://broker.pulsar:6650/ # or pulsar+ssl://pulsar-prod-broker.pulsar:6651/ when using TLS
pulsarWebServiceUrl: http://broker.pulsar:8080/ # or https://pulsar-prod-broker.pulsar:8443/ when using TLS
useTls: true # when using TLS, critical!

```

#### With authentication

When running a functions worker in a standalone process (that is, not embedded in the broker) in a cluster with authentication, you must configure your functions worker to both be able to interact with the broker *and* also to authenticate incoming requests as well. This requires many of the same properties to be set that the broker requires for authentication or authorization.

As an example, assuming you want to use token authentication. Here is an example of properties you need to set in `function-worker.yml`

```Yaml
clientAuthenticationPlugin: org.apache.pulsar.client.impl.auth.AuthenticationToken
clientAuthenticationParameters: file:///etc/pulsar/token/admin-token.txt
configurationStoreServers: zookeeper-cluster:2181 # auth requires a connection to zookeeper
authenticationProviders:
 - "org.apache.pulsar.broker.authentication.AuthenticationProviderToken"
authorizationEnabled: true
authenticationEnabled: true
superUserRoles:
  - superuser
  - proxy
properties:
  tokenSecretKey: file:///etc/pulsar/jwt/secret # if using a secret token
  tokenPublicKey: file:///etc/pulsar/jwt/public.key # if using public/private key tokens
```

> #### Note 
>
> You must configure both the Function Worker authorization or authentication for the server to proper authentication requests and configure the client to be authenticated to communicate with the broker.
