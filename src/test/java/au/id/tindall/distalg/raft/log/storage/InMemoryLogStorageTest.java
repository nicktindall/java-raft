package au.id.tindall.distalg.raft.log.storage;

class InMemoryLogStorageTest extends AbstractLogStorageTest<InMemoryLogStorage> {

    @Override
    protected InMemoryLogStorage createLogStorage() {
        return new InMemoryLogStorage();
    }
}