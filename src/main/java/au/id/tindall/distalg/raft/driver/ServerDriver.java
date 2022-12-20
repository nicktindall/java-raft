package au.id.tindall.distalg.raft.driver;

import au.id.tindall.distalg.raft.Server;

public interface ServerDriver {

    void start(Server<?> server);

    void stop();
}
