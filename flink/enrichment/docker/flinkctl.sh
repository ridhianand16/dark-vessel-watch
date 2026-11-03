#!/usr/bin/env bash
# Submit, stop and restart the enrichment job on the Flink session cluster.
# Runs in the flink-submit container (Makefile: flink-up, flink-savepoint, flink-restart).
#
#   flinkctl.sh submit    start the job unless it is already running; SAVEPOINT=<dir> restores
#   flinkctl.sh stop      stop with a savepoint (sinks commit, state is saved), print its path
#   flinkctl.sh restart   stop with a savepoint if running, then start from the latest savepoint
#
# Stop-with-savepoint, not cancel: it lets the Kafka sinks commit their last transactions, so
# restarting from that savepoint neither repeats nor skips output. Restoring an older savepoint
# while the job had moved on would repeat output, so restart always takes the newest one.
set -euo pipefail

JM=${FLINK_JOBMANAGER:-flink-jobmanager:8081}
JOB_NAME=dvw-enrichment
JAR=/opt/dvw/enrichment.jar
SAVEPOINTS=/flink-data/savepoints

log() { echo "flinkctl: $*" >&2; }

wait_for_jobmanager() {
  for _ in $(seq 1 60); do
    curl -sf "http://$JM/overview" >/dev/null && return 0
    sleep 2
  done
  log "JobManager at $JM did not answer within 120 s"; exit 1
}

# Id of the job if it is running, restarting or scheduled. `flink list` prints lines like
# "04.11.2026 10:00:00 : 3f2a... : dvw-enrichment (RUNNING)" and omits finished jobs.
active_job_id() {
  flink list -m "$JM" 2>/dev/null \
    | awk -F' : ' -v name="$JOB_NAME (" 'index($3, name) == 1 { print $2; exit }'
}

latest_savepoint() {
  local d
  for d in $(ls -dt "$SAVEPOINTS"/savepoint-* 2>/dev/null); do
    [ -f "$d/_metadata" ] && { echo "$d"; return 0; }
  done
  return 1
}

submit() {
  wait_for_jobmanager
  local jid
  jid=$(active_job_id)
  if [ -n "$jid" ]; then
    log "$JOB_NAME is already running ($jid); not submitting again"; return 0
  fi
  if [ -n "${SAVEPOINT:-}" ]; then
    log "starting $JOB_NAME from $SAVEPOINT"
    flink run -m "$JM" -d -s "$SAVEPOINT" "$JAR"
  else
    log "starting $JOB_NAME (Kafka offsets: committed, else earliest)"
    flink run -m "$JM" -d "$JAR"
  fi
}

stop() {
  wait_for_jobmanager
  local jid
  jid=$(active_job_id)
  if [ -z "$jid" ]; then
    log "$JOB_NAME is not running"; return 1
  fi
  log "stopping $jid with a savepoint"
  flink stop -m "$JM" --type canonical --savepointPath "$SAVEPOINTS" "$jid"
}

restart() {
  wait_for_jobmanager
  if [ -n "$(active_job_id)" ]; then
    stop
  fi
  local sp
  sp=$(latest_savepoint) || { log "no savepoint in $SAVEPOINTS; start fresh with: make flink-up"; exit 1; }
  SAVEPOINT=$sp submit
}

case "${1:-}" in
  submit) submit ;;
  stop) stop ;;
  restart) restart ;;
  *) echo "usage: $0 submit|stop|restart" >&2; exit 2 ;;
esac
