package au.id.tindall.distalg.raft.driver;

import au.id.tindall.distalg.raft.Server;

public enum NoOpServerDriver implements ServerDriver {
    INSTANCE;

    @Override
    public void start(Server<?> server) {
        // Do nothing
    }

    @Override
    public void stop() {
        // Do nothing
    }
}
