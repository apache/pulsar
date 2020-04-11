{{/* vim: set filetype=mustache: */}}

{{/*
pulsar home
*/}}
{{- define "pulsar.home" -}}
{{- print "/pulsar" -}}
{{- end -}}

{{/*
Expand the name of the chart.
*/}}
{{- define "pulsar.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "pulsar.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "pulsar.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create the common labels.
*/}}
{{- define "pulsar.standardLabels" -}}
app: {{ template "pulsar.name" . }}
chart: {{ template "pulsar.chart" . }}
release: {{ .Release.Name }}
heritage: {{ .Release.Service }}
cluster: {{ template "pulsar.fullname" . }}
{{- end }}

{{/*
Create the template labels.
*/}}
{{- define "pulsar.template.labels" -}}
app: {{ template "pulsar.name" . }}
release: {{ .Release.Name }}
cluster: {{ template "pulsar.fullname" . }}
{{- end }}

{{/*
Create the match labels.
*/}}
{{- define "pulsar.matchLabels" -}}
app: {{ template "pulsar.name" . }}
release: {{ .Release.Name }}
{{- end }}
