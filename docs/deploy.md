# Deploy kit

What it takes to run the pipeline on a self-managed, air-gapped Kubernetes cluster, and
the optional metrics. **The kit is validated in CI. It has not been installed on a
cluster**: the development laptop has 8 GB and runs the Compose stack. Everything
measured in [results.md](results.md) comes from Compose. The reasons are in
[ADR 0024](adr/0024-deploy-kit-operators-and-validation.md).

## What exists

| Path | What it is |
|---|---|
| `deploy/helm/dark-vessel-watch` | Umbrella chart: our services, the Jobs, and custom resources for the three operators |
| `deploy/helm/dark-vessel-watch/values-local.yaml` | A single-node cluster: one replica of everything, `ReadWriteOnce` volumes, images loaded into the node |
| `deploy/helm/dark-vessel-watch/values-prod.yaml` | Air-gapped cluster: images from `registry.internal:5000`, 3 Redpanda brokers, a PostGIS replica, observability on |
| `deploy/docker/jobs.Dockerfile` | `dvw-jobs`: migrations, zone package load, replayer, reconciliation |
| `deploy/images.txt` | Every image to mirror, including the operators and the build bases |
| `deploy/observability/` | Prometheus scrape config and the Grafana dashboard, shared by Compose and the chart |
| `.github/workflows/validate.yml` | The CI checks below |

### In the chart

| Part | Kubernetes object | Memory (limit = request) |
|---|---|---|
| raw sink | Deployment | 384Mi |
| tracks and alerts sink | Deployment | 320Mi |
| vessels sink | Deployment | 192Mi |
| tracker | Deployment, `Recreate`, RocksDB on an `emptyDir` (rebuilt from the changelog) | 512Mi |
| Flink enrichment job | `FlinkDeployment`: application mode, parallelism 4, 3 s exactly-once checkpoints, savepoint upgrades, Kubernetes HA | JobManager 768m, TaskManager 1280m |
| ClickHouse 24.8 | `ClickHouseInstallation`, one shard and one replica | 1280Mi |
| PostgreSQL 16 + PostGIS 3.4 | CloudNativePG `Cluster` | 512Mi |
| Redpanda v26.2.6 | Redpanda chart 26.2.5 (`bundle.redpanda`) | 768Mi, `--memory=512M` |
| Topics, 6 partitions each | Hook Job, checks partitions and the transaction timeout | 128Mi |
| ClickHouse and PostGIS migrations | Hook Job | 256Mi |
| Zone package load | Hook Job, `jobs.refdataLoad.enabled` | 512Mi |
| Replay, reconciliation | Jobs per run, `jobs.replay.*`, `jobs.reconcile.*` | 512Mi each |

The memory limits are the Compose `mem_limit`s. The Job limits are estimates, since
these run on the host in Compose and were never capped there.

The hooks are idempotent: topics are created `--if-not-exists` and checked, migrations
skip whatever `schema_migrations` records, and loading the active package again changes
nothing. The replay and reconcile Jobs have `backoffLimit: 0`: a retried replay would
be a second run with a new `run_id`, and a failed reconciliation is a verdict, not a crash.

## Installing

Install the operators first. They are cluster-scoped and not part of the chart:

| Operator | Chart | Version |
|---|---|---|
| Altinity ClickHouse operator | `altinity-clickhouse-operator` from https://docs.altinity.com/clickhouse-operator/ | 0.27.4 |
| CloudNativePG | `cloudnative-pg` from https://cloudnative-pg.github.io/charts | 0.29.1 (operator 1.30.1) |
| Flink Kubernetes Operator | `flink-kubernetes-operator` from https://downloads.apache.org/flink/flink-kubernetes-operator-1.16.1/ | 1.16.1 |

For an air gap, on a machine with internet access:

1. Mirror every image in `deploy/images.txt` (the command is at the top of the file).
   The four `dvw-*` images are built from this repository and pushed directly.
2. Run `helm dependency build deploy/helm/dark-vessel-watch` and `helm pull` the three
   operator charts. Carry the `.tgz` files across with the images.
3. Copy the AIS CSV and a built zone package onto the volumes named by `data.aisClaim`
   and `data.refdataClaim`.

Then, inside:

```
helm install dvw deploy/helm/dark-vessel-watch -n dvw --create-namespace \
  -f deploy/helm/dark-vessel-watch/values-prod.yaml --set jobs.refdataLoad.enabled=true
helm upgrade dvw deploy/helm/dark-vessel-watch -n dvw --reuse-values \
  --set jobs.replay.enabled=true --set jobs.replay.runName=day1
```

The Flink job's `main()` waits for an active zone package before it builds the job,
as in Compose ([ADR 0018](adr/0018-flink-zone-tagging-read-at-startup.md)).

## Validation in CI

`make validate-deploy` runs locally with only Docker, and CI runs the same target:

- `helm lint --strict` with each values file
- `helm template` with each values file, and with every option on, piped to
  `kubeconform -strict` (Kubernetes 1.31 schemas, CRD schemas for the operator resources
  pinned to one CRDs-catalog commit)
- every image the chart renders, with every option on, must appear in `deploy/images.txt`
- `promtool check config` on the scrape config
- the dashboard JSON parses

CI also builds the jobs image and starts each of its entry points, and runs
`make test-py` and `make typecheck`.

What this does not prove: that the operators accept these resources at run time, that
the pods start, or that the job reaches RUNNING. The next step would be a kind cluster
in CI with the operators installed.

## Observability

Off by default, everywhere. With it off, no metrics server starts, no metrics library is
loaded and no monitoring container runs. `make up` uses the same memory as before.

| Source | Endpoint | What |
|---|---|---|
| Sinks | `:9108/metrics` (`METRICS_PORT`) | `dvw_sink_*{sink}`: records, rows, skips, write results, write and commit time, retries, failed commits, last commit time |
| Tracker | `:9108/metrics` | `dvw_tracker_events_total{event}` (the counter in its log line, including the Flink leak counters), batch time and size |
| Flink | `:9249/metrics` | Prometheus reporter: checkpoints, restarts, Kafka source lag, records per operator, heap |
| ClickHouse | `:9363/metrics` | built-in endpoint: `system.metrics`, `system.events`, `system.asynchronous_metrics` |
| Redpanda | `:9644/public_metrics` | broker metrics, and consumer group lag once `enable_consumer_group_metrics` includes `consumer_lag` |

The Python services only serve metrics when `METRICS_PORT` is set
(`common/metrics.py`). Flink's reporter is always configured but only started when
`metrics.reporters` names it.

**Compose:** `make observability` recreates the Python services with `METRICS_PORT`. It
stops the Flink job with a savepoint, recreates the cluster with the reporter on and
resubmits the job from that savepoint. It also turns on Redpanda's lag metrics and
starts Prometheus (http://localhost:9090) and Grafana (http://localhost:3000,
dashboard "Dark Vessel Watch pipeline"), about 512 MiB more. `make observability-down`
reverses it. `make up` afterwards recreates the Python services without metrics.

**Kubernetes:** `observability.enabled=true` sets `METRICS_PORT`, starts the Flink
reporter and adds PodMonitors for our pods, Flink and ClickHouse. It also adds the
dashboard as a ConfigMap for Grafana's sidecar and installs kube-prometheus-stack. On a
cluster that already runs the Prometheus Operator, set `observability.enabled=true` and
remove the subchart with `--set kube-prometheus-stack.enabled=false`.
