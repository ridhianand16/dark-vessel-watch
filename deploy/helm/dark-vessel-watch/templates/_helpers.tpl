{{/* Release-scoped name prefix. */}}
{{- define "dvw.name" -}}
{{- .Release.Name | trunc 40 | trimSuffix "-" -}}
{{- end -}}

{{- define "dvw.labels" -}}
app.kubernetes.io/part-of: dark-vessel-watch
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{/* An image reference with the mirror registry in front. Usage: include "dvw.image" (list $ "sinks") */}}
{{- define "dvw.image" -}}
{{- $root := index . 0 -}}
{{- $ref := index $root.Values.images (index . 1) -}}
{{- with $root.Values.global.imageRegistry -}}{{ . }}/{{- end -}}{{ $ref }}
{{- end -}}

{{- define "dvw.brokers" -}}
{{- if .Values.kafka.brokers -}}
{{ .Values.kafka.brokers }}
{{- else -}}
{{- /* The Redpanda chart's internal Kafka listener: <fullname>.<namespace>.svc:9093. */ -}}
{{ .Values.redpanda.fullnameOverride }}.{{ .Release.Namespace }}.svc.cluster.local:9093
{{- end -}}
{{- end -}}

{{/* ClickHouse service: the operator names it clickhouse-<installation name>. */}}
{{- define "dvw.clickhouseHost" -}}
{{- if .Values.clickhouse.host -}}{{ .Values.clickhouse.host }}{{- else -}}clickhouse-{{ include "dvw.name" . }}{{- end -}}
{{- end -}}

{{- define "dvw.clickhouseSecret" -}}
{{- .Values.clickhouse.existingSecret | default (printf "%s-clickhouse" (include "dvw.name" .)) -}}
{{- end -}}

{{/* PostGIS: CloudNativePG's read-write service is <cluster>-rw, its app secret <cluster>-app. */}}
{{- define "dvw.postgisCluster" -}}{{ include "dvw.name" . }}-postgis{{- end -}}

{{- define "dvw.postgisHost" -}}
{{- if .Values.postgis.host -}}{{ .Values.postgis.host }}{{- else -}}{{ include "dvw.postgisCluster" . }}-rw{{- end -}}
{{- end -}}

{{- define "dvw.postgisSecret" -}}
{{- .Values.postgis.existingSecret | default (printf "%s-app" (include "dvw.postgisCluster" .)) -}}
{{- end -}}

{{/* Settings every service and Job reads (common/env.py): from the shared ConfigMap,
     credentials from the two secrets. */}}
{{- define "dvw.env" -}}
- name: CLICKHOUSE_USER
  value: {{ .Values.clickhouse.user | quote }}
- name: CLICKHOUSE_PASSWORD
  valueFrom:
    secretKeyRef: { name: {{ include "dvw.clickhouseSecret" . }}, key: password }
- name: POSTGRES_USER
  valueFrom:
    secretKeyRef: { name: {{ include "dvw.postgisSecret" . }}, key: username }
- name: POSTGRES_PASSWORD
  valueFrom:
    secretKeyRef: { name: {{ include "dvw.postgisSecret" . }}, key: password }
{{- end -}}

{{- define "dvw.envFrom" -}}
- configMapRef:
    name: {{ include "dvw.name" . }}-env
{{- end -}}

{{- define "dvw.podSecurity" -}}
runAsNonRoot: true
runAsUser: 65534
runAsGroup: 65534
fsGroup: 65534
seccompProfile: { type: RuntimeDefault }
{{- end -}}

{{- define "dvw.containerSecurity" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities: { drop: [ALL] }
{{- end -}}

{{/* Requests equal limits: the scheduler reserves what Compose caps. */}}
{{- define "dvw.resources" -}}
{{- $mem := index . 0 -}}{{- $cpu := index . 1 -}}
requests: { memory: {{ $mem }}, cpu: {{ $cpu | quote }} }
limits: { memory: {{ $mem }} }
{{- end -}}
