package au.id.tindall.distalg.raft.log.storage;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class DumpLog implements Runnable {

    @SuppressWarnings("java:S106")
    public static void main(String[] args) {
        new DumpLog(System.out, Path.of(args[0])).run();
    }

    private final PrintStream printStream;
    private final Path logFilePath;

    public DumpLog(PrintStream printStream, Path logFilePath) {
        this.printStream = printStream;
        this.logFilePath = logFilePath;
    }

    @Override
    public void run() {
        if (!Files.exists(logFilePath)) {
            printStream.println("No log file at " + logFilePath);
            System.exit(1);
        }
        final PersistentLogStorage persistentLogStorage = new PersistentLogStorage(logFilePath);
        for (int i = persistentLogStorage.getFirstLogIndex(); i <= persistentLogStorage.getLastLogIndex(); i++) {
            printStream.format("%d: %s%n", i, persistentLogStorage.getEntry(i));
        }
    }
}
