package dvw.enrichment.zones;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class SharedZonesTest {
    @AfterEach
    void reset() {
        SharedZones.reset();
    }

    @Test
    void fourTasksGetOneIndex() throws Exception {
        TestZones zones = ZoneIndexTest.AREA;
        ExecutorService pool = Executors.newFixedThreadPool(4);
        List<Future<ZoneIndex>> tasks = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            tasks.add(pool.submit(() -> {
                SharedZones.Loading loading = SharedZones.forVersion(TestZones.VERSION);
                for (ZoneElement e : zones.pieces()) {
                    loading.offer(e);
                }
                return loading.complete(zones.marker().seq);
            }));
        }
        ZoneIndex first = tasks.get(0).get();
        for (Future<ZoneIndex> t : tasks) {
            assertSame(first, t.get());
        }
        pool.shutdown();
        assertEquals(zones.pieces().size(), first.pieces);
    }

    @Test
    void missingPieceFailsLoudly() {
        TestZones zones = ZoneIndexTest.AREA;
        SharedZones.Loading loading = SharedZones.forVersion(TestZones.VERSION);
        zones.pieces().stream().skip(1).forEach(loading::offer);
        assertThrows(IllegalStateException.class, () -> loading.complete(zones.marker().seq));
    }

    @Test
    void newVersionReplacesTheOld() {
        SharedZones.Loading a = SharedZones.forVersion("a");
        assertSame(a, SharedZones.forVersion("a"));
        SharedZones.Loading b = SharedZones.forVersion("b");
        assertEquals("b", b.version);
        assertEquals("a", a.version);
    }
}
