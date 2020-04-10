{{/*
Define the pulsar toolset service
*/}}
{{- define "pulsar.toolset.service" -}}
{{ template "pulsar.fullname" . }}-{{ .Values.toolset.component }}
{{- end }}

{{/*
Define the toolset hostname
*/}}
{{- define "pulsar.toolset.hostname" -}}
${HOSTNAME}.{{ template "pulsar.toolset.service" . }}.{{ .Values.namespace }}.svc.cluster.local
{{- end -}}

{{/*
Define toolset zookeeper client tls settings
*/}}
{{- define "pulsar.toolset.zookeeper.tls.settings" -}}
{{- if and .Values.tls.enabled .Values.tls.zookeeper.enabled -}}
/pulsar/keytool/keytool.sh toolset {{ template "pulsar.toolset.hostname" . }} true;
{{- end -}}
{{- end }}

{{/*
Define toolset tls certs mounts
*/}}
{{- define "pulsar.toolset.certs.volumeMounts" -}}
{{- if and .Values.tls.enabled .Values.tls.zookeeper.enabled }}
- name: toolset-certs
  mountPath: "/pulsar/certs/toolset"
  readOnly: true
- name: ca
  mountPath: "/pulsar/certs/ca"
  readOnly: true
{{- if .Values.tls.zookeeper.enabled }}
- name: keytool
  mountPath: "/pulsar/keytool/keytool.sh"
  subPath: keytool.sh
{{- end }}
{{- end }}
{{- end }}

{{/*
Define toolset tls certs volumes
*/}}
{{- define "pulsar.toolset.certs.volumes" -}}
{{- if and .Values.tls.enabled .Values.tls.zookeeper.enabled }}
- name: toolset-certs
  secret:
    secretName: "{{ template "pulsar.fullname" . }}-{{ .Values.tls.toolset.cert_name }}"
    items:
    - key: tls.crt
      path: tls.crt
    - key: tls.key
      path: tls.key
- name: ca
  secret:
    secretName: "{{ template "pulsar.fullname" . }}-ca-tls"
    items:
    - key: ca.crt
      path: ca.crt
{{- if .Values.tls.zookeeper.enabled }}
- name: keytool
  configMap:
    name: "{{ template "pulsar.fullname" . }}-keytool-configmap"
    defaultMode: 0755
{{- end }}
{{- end }}
{{- end }}