package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.state.Snapshot;

import static java.lang.String.format;

public enum BufferedTruncationCalculator {
    ;

    /**
     * This is stupidly complicated, so do it once and reuse it
     *
     * @param snapshot         the snapshot being installed
     * @param logStorage       the LogStorage being truncated
     * @param truncationBuffer the truncationBuffer setting
     * @return The calculated truncation details
     */
    public static TruncationDetails calculateTruncation(Snapshot snapshot, LogStorage logStorage, int truncationBuffer) {
        if (truncationBuffer < 0) {
            throw new IllegalArgumentException(format("Truncation buffer must be greater than zero (was %d)", truncationBuffer));
        }
        int newPrevIndex = snapshot.getLastIndex();
        int entriesBeingTruncated = logStorage.size();
        Term newPrevTerm = snapshot.getLastTerm();
        if (newPrevIndex <= logStorage.getLastLogIndex()) {
            newPrevIndex = Math.max(logStorage.getPrevIndex(), newPrevIndex - truncationBuffer);
            if (newPrevIndex == logStorage.getPrevIndex()) {
                newPrevTerm = logStorage.getPrevTerm();
            } else if (newPrevIndex == snapshot.getLastIndex()) {
                newPrevTerm = snapshot.getLastTerm();
            } else {
                newPrevTerm = logStorage.getEntry(newPrevIndex).getTerm();
            }
            entriesBeingTruncated = newPrevIndex - logStorage.getPrevIndex();
        }
        return new TruncationDetails(newPrevIndex, newPrevTerm, entriesBeingTruncated);
    }

    public static class TruncationDetails {
        private final int newPrevIndex;
        private final Term newPrevTerm;
        private final int entriesToTruncate;

        public TruncationDetails(int newPrevIndex, Term newPrevTerm, int entriesToTruncated) {
            this.newPrevIndex = newPrevIndex;
            this.newPrevTerm = newPrevTerm;
            this.entriesToTruncate = entriesToTruncated;
        }

        public int getNewPrevIndex() {
            return newPrevIndex;
        }

        public Term getNewPrevTerm() {
            return newPrevTerm;
        }

        public int getEntriesToTruncate() {
            return entriesToTruncate;
        }

        @Override
        public String toString() {
            return "TruncationDetails{" +
                    "newPrevIndex=" + newPrevIndex +
                    ", newPrevTerm=" + newPrevTerm +
                    ", entriesToTruncate=" + entriesToTruncate +
                    '}';
        }
    }
}
