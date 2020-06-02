#!/bin/bash
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


# Checks that appropriate gke params are set and
# that gcloud and kubectl are properly installed and authenticated

function need_tool(){
  local tool="${1}"
  local url="${2}"

  echo >&2 "${tool} is required. Please follow ${url}"
  exit 1
}

function need_gcloud(){
  need_tool "gcloud" "https://cloud.google.com/sdk/downloads"
}

function need_kubectl(){
  need_tool "kubectl" "https://kubernetes.io/docs/tasks/tools/install-kubectl"
}

function need_helm(){
  need_tool "helm" "https://github.com/helm/helm/#install"
}

function need_eksctl(){
  need_tool "eksctl" "https://eksctl.io"
}

function validate_gke_required_tools(){
  if [ -z "$PROJECT" ]; then
    echo "\$PROJECT needs to be set to your project id";
    exit 1;
  fi

  for comm in gcloud kubectl helm
  do
    command  -v "${comm}" > /dev/null 2>&1 || "need_${comm}"
  done

  gcloud container clusters list --project $PROJECT >/dev/null 2>&1 || { echo >&2 "Gcloud seems to be configured incorrectly or authentication is unsuccessfull"; exit 1; }

}

function cluster_admin_password_gke(){
  gcloud container clusters describe $CLUSTER_NAME --zone $ZONE --project $PROJECT --format='value(masterAuth.password)';
}

function validate_eks_required_tools(){
  for comm in eksctl kubectl helm
  do
    command -v "${comm}" > /dev/null 2>&1 || "need_${comm}"
  done
}
