package au.id.tindall.distalg.raft.state;

class InMemoryPersistentStateTest extends PersistentStateContractTest {

    @Override
    protected PersistentState<Integer> createPersistentState() {
        final InMemoryPersistentState<Integer> persistentState = new InMemoryPersistentState<>(PersistentStateContractTest.SERVER_ID);
        persistentState.addSnapshotInstalledListener(persistentState.getLogStorage()::installSnapshot);
        return persistentState;
    }
}