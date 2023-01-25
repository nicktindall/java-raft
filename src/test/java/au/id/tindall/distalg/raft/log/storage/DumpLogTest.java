package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class DumpLogTest {

    @Test
    void willDumpLog(@TempDir Path tempDir) {
        final Path logFilePath = tempDir.resolve("logFile.log");
        PersistentLogStorage persistentLogStorage = new PersistentLogStorage(logFilePath);
        Log log = new Log(persistentLogStorage);
        IntStream.range(0, 4).forEach(i -> {
            log.appendEntries(i, List.of(new ClientRegistrationEntry(new Term(i), 123 + i)));
        });
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(baos);
        new DumpLog(printStream, logFilePath).run();
        assertThat(baos).hasToString(
                "1: ClientRegistrationEntry{clientId=123} LogEntry{term=0}\n" +
                        "2: ClientRegistrationEntry{clientId=124} LogEntry{term=1}\n" +
                        "3: ClientRegistrationEntry{clientId=125} LogEntry{term=2}\n" +
                        "4: ClientRegistrationEntry{clientId=126} LogEntry{term=3}\n");
    }
}