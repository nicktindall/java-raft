package au.id.tindall.distalg.raft.elections;

import java.util.Random;

public class ElectionTimeoutGenerator {

    private final Random random;
    private final long minimumElectionTimeoutInMilliseconds;
    private final long maximumElectionTimeoutInMilliseconds;

    public ElectionTimeoutGenerator(Random random, long minimumElectionTimeoutInMilliseconds, long maximumElectionTimeoutInMilliseconds) {
        if (maximumElectionTimeoutInMilliseconds <= minimumElectionTimeoutInMilliseconds) {
            throw new IllegalArgumentException("Maximum election timeout must be greater than minimum election timeout");
        }
        if (minimumElectionTimeoutInMilliseconds < 0) {
            throw new IllegalArgumentException("Minimum election timeout must not be negative");
        }
        this.random = random;
        this.minimumElectionTimeoutInMilliseconds = minimumElectionTimeoutInMilliseconds;
        this.maximumElectionTimeoutInMilliseconds = maximumElectionTimeoutInMilliseconds;
    }

    public Long next() {
        int rangeSize = (int) (this.maximumElectionTimeoutInMilliseconds - this.minimumElectionTimeoutInMilliseconds);
        return this.minimumElectionTimeoutInMilliseconds + random.nextInt(rangeSize);
    }
}
