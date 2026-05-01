{{/*
Expand the name of the chart.
*/}}
{{- define "postgresql-trino-gateway.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited
to this (by the DNS naming spec).
*/}}
{{- define "postgresql-trino-gateway.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "postgresql-trino-gateway.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "postgresql-trino-gateway.labels" -}}
helm.sh/chart: {{ include "postgresql-trino-gateway.chart" . }}
{{ include "postgresql-trino-gateway.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
stackable.tech/vendor: stackable
{{- end }}

{{/*
Selector labels
*/}}
{{- define "postgresql-trino-gateway.selectorLabels" -}}
app.kubernetes.io/name: {{ include "postgresql-trino-gateway.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: gateway
{{- end }}

{{/*
Service account name to use.
*/}}
{{- define "postgresql-trino-gateway.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "postgresql-trino-gateway.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Container image reference.
*/}}
{{- define "postgresql-trino-gateway.image" -}}
{{- if .Values.image.registry }}
{{- printf "%s/%s:%s" .Values.image.registry .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) }}
{{- else }}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) }}
{{- end }}
{{- end }}

{{/*
True iff a listener TLS source is configured. Used to gate volume,
mount, and CLI-flag rendering on a single decision.
*/}}
{{- define "postgresql-trino-gateway.tlsEnabled" -}}
{{- if or .Values.tls.secretClass .Values.tls.existingSecret -}}true{{- end -}}
{{- end }}
