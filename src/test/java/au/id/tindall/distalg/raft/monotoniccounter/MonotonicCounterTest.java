package au.id.tindall.distalg.raft.monotoniccounter;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.state.InMemorySnapshot;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.Set;

import static org.apache.logging.log4j.LogManager.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MonotonicCounterTest {

    private static final Logger LOGGER = getLogger();

    @Test
    void canCreateHorriblyInefficientSnapshot() {
        final MonotonicCounter monotonicCounter = new MonotonicCounter();
        for (int i = 0; i < 256; i++) {
            monotonicCounter.apply(i, BigInteger.valueOf(i).toByteArray());
        }
        assertEquals(256, monotonicCounter.getCounter().intValue());
        final byte[] snapshotBytes = monotonicCounter.createSnapshot();
        final InMemorySnapshot inMemorySnapshot = new InMemorySnapshot(1, new Term(1), new ConfigurationEntry(new Term(1), Set.of()));
        inMemorySnapshot.writeBytes(0, snapshotBytes);
        inMemorySnapshot.finalise();
        LOGGER.info("Created snapshot of size " + snapshotBytes.length);
        monotonicCounter.installSnapshot(inMemorySnapshot);
        assertEquals(256, monotonicCounter.getCounter().intValue());
    }
}