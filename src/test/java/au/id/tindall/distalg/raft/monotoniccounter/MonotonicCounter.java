package au.id.tindall.distalg.raft.monotoniccounter;

import au.id.tindall.distalg.raft.state.Snapshot;
import au.id.tindall.distalg.raft.statemachine.StateMachine;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.String.format;
import static org.apache.logging.log4j.LogManager.getLogger;

/**
 * Just checks that the current counter value is the same as the command value,
 * increments the counter and returns the new value.
 * <p>
 * For testing that the state machine behaves as the client expects.
 */
public class MonotonicCounter implements StateMachine {

    private static final Logger LOGGER = getLogger();
    private static final int CHECKSUM_OFFSET = 4;
    private static final int JUNK_OFFSET = 20;
    private static final int VALUE_OFFSET_OFFSET = 0;
    private static final int MIN_SNAPSHOT_SIZE_BYTES = 600_000;
    private static final int MAX_SNAPSHOT_SIZE_BYTES = 1_000_000;
    private static final BigInteger LOG_EVERY_N_VALUES = BigInteger.valueOf(100L);
    private int lastIndexApplied = 0;

    private BigInteger counter = BigInteger.ZERO;

    @Override
    public byte[] apply(int index, byte[] command) {
        if (index <= lastIndexApplied) {
            throw new IllegalStateException(format("Attempted to apply index %,d, when last index applied was %,d, this should never happen", index, lastIndexApplied));
        }
        lastIndexApplied = index;
        BigInteger expected = new BigInteger(command);
        if (!counter.equals(expected)) {
            throw new IllegalStateException(format("Client out of sync! expected %s, state is %s (applying index=%,d)", expected, counter, index));
        }
        counter = counter.add(BigInteger.ONE);
        if (counter.mod(LOG_EVERY_N_VALUES).equals(BigInteger.ZERO)) {
            LOGGER.info("Command successful, new value is {} (index={})", counter, index);
        }
        return counter.toByteArray();
    }

    /**
     * Intentionally horribly inefficient serialisation
     * <p>
     * layout:
     * <pre>
     * 0             4          20     valueOffset  end
     * | valueOffset | checkSum | junk | value      |
     * </pre>
     */
    @Override
    public byte[] createSnapshot() {
        // generate a random length array
        int snapshotSize = ThreadLocalRandom.current().nextInt(MIN_SNAPSHOT_SIZE_BYTES, MAX_SNAPSHOT_SIZE_BYTES);
        ByteBuffer snapshot = ByteBuffer.allocate(snapshotSize);
        byte[] counterValue = counter.toByteArray();
        // fill it with random noise
        ThreadLocalRandom.current().nextBytes(snapshot.array());
        // write the value to the end of it (recording the offset)
        final int valueOffset = snapshotSize - counterValue.length;
        snapshot.position(valueOffset).put(counterValue);
        snapshot.position(VALUE_OFFSET_OFFSET).putInt(valueOffset);
        // write the checksum to the start
        byte[] snapshotChecksum = calculateChecksum(snapshot);
        snapshot.position(CHECKSUM_OFFSET).put(snapshotChecksum);
        return snapshot.array();
    }

    @Override
    public void installSnapshot(Snapshot snapshot) {
        ByteBuffer temporaryBuffer = ByteBuffer.allocate((int) snapshot.getLength() - snapshot.snapshotOffset());
        snapshot.readInto(temporaryBuffer, snapshot.snapshotOffset());
        byte[] snapshotChecksum = calculateChecksum(temporaryBuffer);
        byte[] snapshotArray = temporaryBuffer.array();
        if (!Arrays.equals(snapshotArray, CHECKSUM_OFFSET, JUNK_OFFSET, snapshotChecksum, 0, 16)) {
            throw new IllegalArgumentException("Invalid snapshot detected, bad checksum");
        }
        int valueOffset = temporaryBuffer.getInt(VALUE_OFFSET_OFFSET);
        counter = new BigInteger(Arrays.copyOfRange(snapshotArray, valueOffset, snapshotArray.length));
        LOGGER.info("Installed snapshot, new value={}, index={}", counter, snapshot.getLastIndex());
        lastIndexApplied = snapshot.getLastIndex();
    }

    private byte[] calculateChecksum(ByteBuffer snapshot) {
        try {
            final MessageDigest md5 = MessageDigest.getInstance("MD5");
            snapshot.position(JUNK_OFFSET);
            md5.update(snapshot);
            return md5.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("This won't happen");
        }
    }

    public BigInteger getCounter() {
        return counter;
    }
}
