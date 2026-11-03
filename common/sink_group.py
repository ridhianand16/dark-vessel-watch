"""Run several BatchSinks in one process, one thread each, and stop them together.

Each sink keeps its own consumer, group, batches and commits; nothing is shared but the
process (one Python runtime and image instead of one per topic, see the Compose memory
budget). The delivery loop is unchanged: every sink still commits only after its write.

Fail-stop holds for the group: when one sink's run() returns non-zero or raises, every
other sink is asked to stop (finish the in-flight batch, commit, close) and the process
exits 1, so the container restarts and the failed sink re-reads from its last commit.
A signal stops all of them the same way and the exit code is 0 if all stopped cleanly.

librdkafka and the ClickHouse HTTP client release the GIL while waiting on the network,
so the threads overlap where it matters. Row mapping is Python and does contend: a big
tracks batch can delay an alert insert by the few milliseconds it holds the GIL.
"""

from __future__ import annotations

import logging
import queue
import signal
import threading
from collections.abc import Sequence
from types import FrameType
from typing import Any

from common.kafka_batch import BatchSink

log = logging.getLogger("sink-group")


def run_together(sinks: Sequence[BatchSink[Any]], install_signals: bool = True) -> int:
    """Run every sink until all have stopped. 0 if all stopped cleanly, else 1."""
    done: queue.Queue[tuple[str, int]] = queue.Queue()

    def target(sink: BatchSink[Any]) -> None:
        code = 1
        try:
            code = sink.run()
        except BaseException:
            log.exception("%s sink crashed", sink.name)
        finally:
            done.put((sink.name, code))

    def stop_all(reason: str) -> None:
        for s in sinks:
            if not s.stopping:
                s.stop(reason)

    if install_signals:

        def on_signal(signum: int, _frame: FrameType | None) -> None:
            stop_all(f"signal {signal.Signals(signum).name}")

        signal.signal(signal.SIGTERM, on_signal)
        signal.signal(signal.SIGINT, on_signal)

    threads = [threading.Thread(target=target, args=(s,), name=f"{s.name}-sink") for s in sinks]
    for t in threads:
        t.start()
    worst = 0
    for _ in sinks:
        # get() with a timeout so the main thread wakes up to run signal handlers.
        while True:
            try:
                name, code = done.get(timeout=0.5)
                break
            except queue.Empty:
                continue
        # run() returns 0 only after a stop request, which only stop_all() makes here.
        if code != 0 and worst == 0:
            stop_all(f"{name} sink failed (exit {code})")
        worst = max(worst, code)
    for t in threads:
        t.join()
    return 1 if worst else 0
