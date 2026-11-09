# 0024. The deploy kit: operator-managed stores and Flink, CRs in our chart, validated in CI

Status: accepted (2026-11-09)

## Context

The pipeline runs in Docker Compose on one 8 GB laptop. The target platform is a
self-managed, air-gapped Kubernetes cluster. The deploy kit has to describe that
deployment well enough to be reviewed and installed, but there is no cluster to run it
on: a local Kubernetes next to the Compose stack would not fit in 8 GB.

Four stateful parts need more than a Deployment:

- **ClickHouse**: server config, users and a persistent volume per replica.
- **PostgreSQL with PostGIS**: initdb, credentials, failover once there is a replica.
- **The Flink job**: JobManager and TaskManager pods, checkpoints, and upgrades that stop
  with a savepoint and resume from it (ADR 0014, 0022). In Compose, `flinkctl.sh` does
  this by hand.
- **Redpanda**: a StatefulSet with tuned flags and per-broker addresses.

## Decision

1. **One umbrella chart**, `deploy/helm/dark-vessel-watch`. It holds our services as
   Deployments with the Compose memory limits as requests and limits, and topics and
   migrations as idempotent post-install and post-upgrade hooks. Zone package load,
   replay and reconciliation are opt-in Jobs, from one `dvw-jobs` image.
2. **Operators for the stateful parts.** The chart creates only the custom resources:
   - a `ClickHouseInstallation` for the **Altinity ClickHouse operator** (0.27.4)
   - a `Cluster` for **CloudNativePG** (1.30.1), with the `cloudnative-pg/postgis:16-3.4`
     image
   - a `FlinkDeployment` for the **Flink Kubernetes Operator** (1.16.1) in application
     mode: parallelism 4, 3 s exactly-once checkpoints, the same process sizes, and
     `upgradeMode: savepoint`
3. **Operators are installed per cluster, not as chart dependencies.** They are
   cluster-scoped (CRDs, webhooks, cluster roles) and shared by every release, and
   CloudNativePG installs its CRDs from templates. If they were subcharts, the first
   install would fail: our CRs would reach the API server before the CRDs exist.
   docs/deploy.md pins the operator versions, and deploy/images.txt lists their images.
4. **Redpanda and kube-prometheus-stack are pinned, optional dependencies.** Each has a
   condition (`bundle.redpanda`, `observability.enabled`). Chart.lock records their
   versions and digest, `helm dependency build` vendors them, and the archives are not
   committed. Redpanda keeps the Compose flags: `--smp=1 --memory=512M` in 768Mi.
5. **Flink checkpoints go on a ReadWriteMany volume, not object storage.** The air gap
   rules out S3, and running MinIO only for checkpoints would add a stateful service.
   Kubernetes HA keeps the latest checkpoint pointer in ConfigMaps, so a JobManager
   restart resumes the job (the Compose session cluster cannot).
6. **Validated in CI, not run.** `make validate-deploy` runs helm lint and helm template
   with both values files, and kubeconform in strict mode against Kubernetes 1.31 and
   CRD schemas pinned to one CRDs-catalog commit. It also checks that every rendered
   image is in deploy/images.txt, and runs promtool and a JSON parse of the dashboard.

## Alternatives

- **Plain StatefulSets for ClickHouse and PostgreSQL.** This means fewer moving parts,
  but initdb, users, failover and volume handling become our code. The operators are
  the standard way to run these in production, and both run fully offline.
- **ClickHouse Keeper and replicas now.** The Compose design is single-replica. Replicated
  tables would need `ReplicatedReplacingMergeTree` migrations, a design change that
  deserves its own ADR. The CR has one shard and one replica.
- **Flink session cluster plus a submit Job, as in Compose.** That keeps one code path,
  but loses savepoint upgrades and HA, and repeats by hand what the operator does.
- **Bitnami charts for ClickHouse and PostgreSQL.** They need no operator, but they are
  not what the platform runs (CloudNativePG, Altinity), and Bitnami cut back its free
  image catalogue in 2025, which makes long-term image pins less certain.
- **kind or k3d in CI with a real install.** That would prove more, but needs the operators,
  images built in CI and AIS data, and takes many minutes per run. It is the next step if
  the kit is ever run.

## Consequences

- Installing needs the three operators first. docs/deploy.md gives the order and versions.
- A schema-valid manifest is not a running system. Nothing here checks that the pods
  start, that the operators accept these CRs or that the job reaches RUNNING. Schema
  and lint errors, missing images and broken templates are caught. Behaviour is still
  proven only by the Compose results report.
- The FlinkDeployment and the Compose job share an image and settings but are
  submitted differently. A change to checkpointing or memory must be made in both:
  `FLINK_PROPERTIES` in Compose and `flinkConfiguration` in the chart.
- Kubernetes needs a ReadWriteMany storage class for Flink state and the runs volume,
  except on a single node (values-local.yaml uses ReadWriteOnce).
