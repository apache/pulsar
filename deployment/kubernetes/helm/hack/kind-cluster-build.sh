#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

PULSAR_CHART_HOME=$(unset CDPATH && cd $(dirname "${BASH_SOURCE[0]}")/.. && pwd)
cd ${PULSAR_CHART_HOME}

source ${PULSAR_CHART_HOME}/hack/common.sh

hack::ensure_kubectl
hack::ensure_helm

usage() {
    cat <<EOF
This script use kind to create Kubernetes cluster, about kind please refer: https://kind.sigs.k8s.io/
Before run this script, please ensure that:
* have installed docker
* have installed kind and kind's version == ${KIND_VERSION}
Options:
       -h,--help               prints the usage message
       -n,--name               name of the Kubernetes cluster,default value: kind
       -c,--nodeNum            the count of the cluster nodes,default value: 6
       -k,--k8sVersion         version of the Kubernetes cluster,default value: v1.12.8
       -v,--volumeNum          the volumes number of each kubernetes node,default value: 9
Usage:
    $0 --name testCluster --nodeNum 4 --k8sVersion v1.12.9
EOF
}

while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -n|--name)
    clusterName="$2"
    shift
    shift
    ;;
    -c|--nodeNum)
    nodeNum="$2"
    shift
    shift
    ;;
    -k|--k8sVersion)
    k8sVersion="$2"
    shift
    shift
    ;;
    -v|--volumeNum)
    volumeNum="$2"
    shift
    shift
    ;;
    -h|--help)
    usage
    exit 0
    ;;
    *)
    echo "unknown option: $key"
    usage
    exit 1
    ;;
esac
done

clusterName=${clusterName:-pulsar-dev}
nodeNum=${nodeNum:-6}
k8sVersion=${k8sVersion:-v1.14.10}
volumeNum=${volumeNum:-9}

echo "clusterName: ${clusterName}"
echo "nodeNum: ${nodeNum}"
echo "k8sVersion: ${k8sVersion}"
echo "volumeNum: ${volumeNum}"

# check requirements
for requirement in kind docker
do
    echo "############ check ${requirement} ##############"
    if hash ${requirement} 2>/dev/null;then
        echo "${requirement} have installed"
    else
        echo "this script needs ${requirement}, please install ${requirement} first."
        exit 1
    fi
done

echo "############# start create cluster:[${clusterName}] #############"
workDir=${HOME}/kind/${clusterName}
mkdir -p ${workDir}

data_dir=${workDir}/data

echo "clean data dir: ${data_dir}"
if [ -d ${data_dir} ]; then
    rm -rf ${data_dir}
fi

configFile=${workDir}/kind-config.yaml

cat <<EOF > ${configFile}
kind: Cluster
apiVersion: kind.sigs.k8s.io/v1alpha3
nodes:
- role: control-plane
  extraPortMappings:
  - containerPort: 5000
    hostPort: 5000
    listenAddress: 127.0.0.1
    protocol: TCP
EOF

for ((i=0;i<${nodeNum};i++))
do
    mkdir -p ${data_dir}/worker${i}
    cat <<EOF >>  ${configFile}
- role: worker
  extraMounts:
EOF
    for ((k=1;k<=${volumeNum};k++))
    do
        mkdir -p ${data_dir}/worker${i}/vol${k}
        cat <<EOF >> ${configFile}
  - containerPath: /mnt/disks/vol${k}
    hostPath: ${data_dir}/worker${i}/vol${k}
EOF
    done
done

matchedCluster=$(kind get clusters | grep ${clusterName})
if [[ "${matchedCluster}" == "${clusterName}" ]]; then
    echo "Kind cluster ${clusterName} already exists"
    kind delete cluster --name=${clusterName}
fi
echo "start to create k8s cluster"
kind create cluster --config ${configFile} --image kindest/node:${k8sVersion} --name=${clusterName}
export KUBECONFIG=${workDir}/kubeconfig.yaml
kind get kubeconfig --name=${clusterName} > ${KUBECONFIG}

echo "deploy docker registry in kind"
registryNode=${clusterName}-control-plane
registryNodeIP=$($KUBECTL_BIN get nodes ${registryNode} -o template --template='{{range.status.addresses}}{{if eq .type "InternalIP"}}{{.address}}{{end}}{{end}}')
registryFile=${workDir}/registry.yaml

cat <<EOF >${registryFile}
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: registry
spec:
  selector:
    matchLabels:
      app: registry
  template:
    metadata:
      labels:
        app: registry
    spec:
      hostNetwork: true
      nodeSelector:
        kubernetes.io/hostname: ${registryNode}
      tolerations:
      - key: node-role.kubernetes.io/master
        operator: "Equal"
        effect: "NoSchedule"
      containers:
      - name: registry
        image: registry:2
        volumeMounts:
        - name: data
          mountPath: /data
      volumes:
      - name: data
        hostPath:
          path: /data
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: registry-proxy
  labels:
    app: registry-proxy
spec:
  selector:
    matchLabels:
      app: registry-proxy
  template:
    metadata:
      labels:
        app: registry-proxy
    spec:
      hostNetwork: true
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchExpressions:
              - key: kubernetes.io/hostname
                operator: NotIn
                values:
                  - ${registryNode}
      tolerations:
      - key: node-role.kubernetes.io/master
        operator: "Equal"
        effect: "NoSchedule"
      containers:
        - name: socat
          image: alpine/socat:1.0.5
          args:
          - tcp-listen:5000,fork,reuseaddr
          - tcp-connect:${registryNodeIP}:5000
EOF
$KUBECTL_BIN apply -f ${registryFile}

echo "init pulsar  env"
$KUBECTL_BIN apply -f ${PULSAR_CHART_HOME}/manifests/local-dind/local-volume-provisioner.yaml

docker pull gcr.io/google-containers/kube-scheduler:${k8sVersion}
docker tag gcr.io/google-containers/kube-scheduler:${k8sVersion} mirantis/hypokube:final
kind load docker-image --name=${clusterName} mirantis/hypokube:final

echo "############# success create cluster:[${clusterName}] #############"

echo "To start using your cluster, run:"
echo "    export KUBECONFIG=${KUBECONFIG}"
echo ""
echo <<EOF
NOTE: In kind, nodes run docker network and cannot access host network.
If you configured local HTTP proxy in your docker, images may cannot be pulled
because http proxy is inaccessible.
If you cannot remove http proxy settings, you can either whitelist image
domains in NO_PROXY environment or use 'docker pull <image> && kind load
docker-image <image>' command to load images into nodes.
EOF