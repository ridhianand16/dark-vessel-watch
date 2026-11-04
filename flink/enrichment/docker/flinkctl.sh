#!/usr/bin/env bash
# Submit, stop and restart the enrichment job on the Flink session cluster.
# Runs in the flink-submit container (Makefile: flink-up, flink-savepoint, flink-restart).
#
#   flinkctl.sh submit    start the job unless it is already running, from the newest retained
#                         state (SAVEPOINT=<dir> picks one); fresh only when there is none
#   flinkctl.sh stop      stop with a savepoint (sinks commit, state is saved), print its path
#   flinkctl.sh restart   stop with a savepoint if running, then start from the newest state
#
# Stop-with-savepoint, not cancel: it lets the Kafka sinks commit their last transactions, so
# restarting from that savepoint neither repeats nor skips output.
#
# The newest state is the most recent _metadata among the savepoints and the retained
# checkpoints (docs/adr/0022). The session cluster has no high availability: a JobManager
# restart drops the job, and only its checkpoints on the flink-data volume remain. Starting
# fresh from the group's committed offsets instead would lose the dedup and zone-buffer state
# and any output whose transaction the restart aborted; restoring an older state than the
# newest would repeat output.
set -euo pipefail

JM=${FLINK_JOBMANAGER:-flink-jobmanager:8081}
JOB_NAME=dvw-enrichment
JAR=/opt/dvw/enrichment.jar
SAVEPOINTS=/flink-data/savepoints
CHECKPOINTS=/flink-data/checkpoints

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

# Directory of the newest completed savepoint or retained checkpoint. A checkpoint's
# _metadata is written last, when the checkpoint completes, so an unfinished one is skipped.
latest_state() {
  local newest
  newest=$(ls -t "$SAVEPOINTS"/savepoint-*/_metadata "$CHECKPOINTS"/*/chk-*/_metadata 2>/dev/null \
    | head -n 1) || true
  [ -n "$newest" ] || return 1
  dirname "$newest"
}

submit() {
  wait_for_jobmanager
  local jid
  jid=$(active_job_id)
  if [ -n "$jid" ]; then
    log "$JOB_NAME is already running ($jid); not submitting again"; return 0
  fi
  local state=${SAVEPOINT:-}
  if [ -z "$state" ]; then
    state=$(latest_state) || state=""
  fi
  if [ -n "$state" ]; then
    log "starting $JOB_NAME from $state"
    flink run -m "$JM" -d -s "$state" "$JAR"
  else
    log "no savepoint or checkpoint on the volume; starting $JOB_NAME fresh (Kafka offsets: committed, else earliest)"
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
  local state
  state=$(latest_state) || { log "no savepoint or checkpoint on the volume; start fresh with: make flink-up"; exit 1; }
  SAVEPOINT=$state submit
}

case "${1:-}" in
  submit) submit ;;
  stop) stop ;;
  restart) restart ;;
  *) echo "usage: $0 submit|stop|restart" >&2; exit 2 ;;
esac
