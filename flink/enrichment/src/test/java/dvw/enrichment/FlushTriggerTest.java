package dvw.enrichment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collection;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TriggerTestHarness;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.Test;

class FlushTriggerTest {
    final TimeWindow w = new TimeWindow(10_000, 20_000);

    TriggerTestHarness<Object, TimeWindow> harness() throws Exception {
        return new TriggerTestHarness<>(new FlushTrigger(5_000), new TimeWindow.Serializer());
    }

    @Test
    void firesAndPurgesWhenTheWatermarkPassesTheWindowEnd() throws Exception {
        var h = harness();
        assertEquals(TriggerResult.CONTINUE, h.processElement(new StreamRecord<>("d", 12_000), w));
        assertEquals(0, h.advanceWatermark(19_998).size());
        Collection<Tuple2<TimeWindow, TriggerResult>> fired = h.advanceWatermark(19_999);
        assertEquals(1, fired.size());
        assertEquals(TriggerResult.FIRE_AND_PURGE, fired.iterator().next().f1);
    }

    @Test
    void flushesOnWallClockWhenTheWatermarkStalls() throws Exception {
        var h = harness();
        h.advanceProcessingTime(1_000);
        h.processElement(new StreamRecord<>("d", 12_000), w);
        h.processElement(new StreamRecord<>("d", 13_000), w);
        assertEquals(1, h.numProcessingTimeTimers(), "elements of one flush period share a timer");
        assertEquals(0, h.advanceProcessingTime(4_999).size());
        Collection<Tuple2<TimeWindow, TriggerResult>> fired = h.advanceProcessingTime(5_000);
        assertEquals(1, fired.size());
        assertEquals(TriggerResult.FIRE_AND_PURGE, fired.iterator().next().f1);
    }

    @Test
    void lateWindowDoesNotFireOnEveryElement() throws Exception {
        var h = harness();
        h.advanceWatermark(60_000); // the window's end is long past (same day replayed again)
        for (int i = 0; i < 100; i++) {
            assertEquals(TriggerResult.CONTINUE, h.processElement(new StreamRecord<>("d", 12_000), w));
        }
        assertEquals(0, h.numEventTimeTimers(), "no event-time timer in the past");
        assertEquals(1, h.advanceProcessingTime(5_000).size(), "one flush for all of them");
    }

    @Test
    void clearRemovesTheEventTimeTimer() throws Exception {
        var h = harness();
        h.processElement(new StreamRecord<>("d", 12_000), w);
        assertEquals(1, h.numEventTimeTimers());
        h.clearTriggerState(w);
        assertEquals(0, h.numEventTimeTimers());
    }
}
