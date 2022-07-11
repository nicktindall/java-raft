package au.id.tindall.distalg.raft.monotoniccounter;

import au.id.tindall.distalg.raft.statemachine.StateMachine;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;

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

    private BigInteger counter = BigInteger.ZERO;

    @Override
    public byte[] apply(byte[] command) {
        BigInteger expected = new BigInteger(command);
        if (!counter.equals(expected)) {
            throw new IllegalStateException(format("Client out of sync! expected %s, state is %s", expected, counter));
        }
        counter = counter.add(BigInteger.ONE);
        Level level = counter.mod(BigInteger.valueOf(100L)).equals(BigInteger.ZERO) ? Level.WARN : Level.DEBUG;
        LOGGER.log(level, "Command successful, new value is {}", counter);
        return counter.toByteArray();
    }

    @Override
    public byte[] createSnapshot() {
        return counter.toByteArray();
    }

    @Override
    public void installSnapshot(byte[] snapshot) {
        counter = new BigInteger(snapshot);
        LOGGER.warn("Installed snapshot, new value=" + counter);
    }

    public BigInteger getCounter() {
        return counter;
    }
}
