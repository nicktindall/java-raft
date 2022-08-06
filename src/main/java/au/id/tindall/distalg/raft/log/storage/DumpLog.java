package au.id.tindall.distalg.raft.log.storage;

import java.nio.file.Files;
import java.nio.file.Path;

public class DumpLog implements Runnable {

    public static void main(String[] args) {
        new DumpLog(Path.of(args[0])).run();
    }

    private final Path logFilePath;

    public DumpLog(Path logFilePath) {
        this.logFilePath = logFilePath;
    }

    @Override
    public void run() {
        if (!Files.exists(logFilePath)) {
            System.out.println("No log file at " + logFilePath);
            System.exit(1);
        }
        final PersistentLogStorage persistentLogStorage = new PersistentLogStorage(logFilePath);
        for (int i = persistentLogStorage.getFirstLogIndex(); i <= persistentLogStorage.getLastLogIndex(); i++) {
            System.out.format("%d: %s%n", i, persistentLogStorage.getEntry(i));
        }
    }
}
