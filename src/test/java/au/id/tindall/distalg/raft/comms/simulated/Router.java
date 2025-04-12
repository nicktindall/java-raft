package au.id.tindall.distalg.raft.comms.simulated;

import java.util.Map;

interface Router<I> {

    boolean route(Map<I, QueueingCluster<I>> clusters);
}
