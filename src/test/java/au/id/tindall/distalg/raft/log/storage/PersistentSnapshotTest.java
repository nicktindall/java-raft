package au.id.tindall.distalg.raft.log.storage;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.serialisation.IntegerIDSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

class PersistentSnapshotTest {

    @Test
    void canPopulateAndFinalise(@TempDir Path tempDir) {
        ConfigurationEntry lastConfig = createConfigurationEntry();
        final Term lastTerm = new Term(456);
        final int lastIndex = 123;
        final Path snapshotPath = tempDir.resolve("snapshot.snap");
        final byte[] contentBytes = "ONE".getBytes(StandardCharsets.UTF_8);

        createAndPopulateSnapshot(snapshotPath, lastTerm, lastIndex, lastConfig, contentBytes);

        try (PersistentSnapshot loadedSnapshot = PersistentSnapshot.load(IntegerIDSerializer.INSTANCE, snapshotPath)) {
            assertThat(loadedSnapshot.getLastTerm()).isEqualTo(lastTerm);
            assertThat(loadedSnapshot.getLastIndex()).isEqualTo(lastIndex);
            assertThat(loadedSnapshot.getLastConfig()).usingRecursiveComparison().isEqualTo(lastConfig);
            byte[] contentBytesRead = new byte[contentBytes.length];
            loadedSnapshot.readInto(ByteBuffer.wrap(contentBytesRead), 0);
            assertThat(contentBytesRead).isEqualTo(contentBytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void canReadContents(@TempDir Path tempDir) {
        ConfigurationEntry lastConfig = createConfigurationEntry();
        final Term lastTerm = new Term(456);
        final int lastIndex = 123;
        final Path snapshotPath = tempDir.resolve("snapshot.snap");
        final int totalLength = 1234;
        final byte[] contentBytes = new byte[totalLength];
        ThreadLocalRandom.current().nextBytes(contentBytes);

        createAndPopulateSnapshot(snapshotPath, lastTerm, lastIndex, lastConfig, contentBytes);

        try (PersistentSnapshot loadedSnapshot = PersistentSnapshot.load(IntegerIDSerializer.INSTANCE, snapshotPath)) {
            int bufferSize = 68;
            byte[] buffer = new byte[bufferSize];
            for (int i = 0; i < Math.ceil((double) totalLength / bufferSize); i++) {
                int expectedStartPosition = i * bufferSize;
                int expectedEndPosition = Math.min(bufferSize * (i + 1), contentBytes.length);
                int expectedReadCount = expectedEndPosition - expectedStartPosition;
                final int actualRead = loadedSnapshot.readInto(ByteBuffer.wrap(buffer), i * bufferSize);
                assertThat(actualRead)
                        .isEqualTo(expectedReadCount);
                assertThat(Arrays.copyOf(buffer, actualRead)).isEqualTo(Arrays.copyOfRange(contentBytes, expectedStartPosition,
                        expectedEndPosition));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void canWriteContentsRandomly(@TempDir Path tempDir) {
        ConfigurationEntry lastConfig = createConfigurationEntry();
        final Term lastTerm = new Term(456);
        final int lastIndex = 123;
        final Path snapshotPath = tempDir.resolve("snapshot.snap");

        try (final PersistentSnapshot persistentSnapshot = PersistentSnapshot.create(IntegerIDSerializer.INSTANCE, snapshotPath, lastIndex, lastTerm, lastConfig)) {
            persistentSnapshot.writeBytes(0, "Testing".getBytes());
            persistentSnapshot.writeBytes(0, "Fa".getBytes());
            persistentSnapshot.writeBytes(4, "er".getBytes());
            persistentSnapshot.writeBytes(6, "!!!".getBytes());
            persistentSnapshot.finalise();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        try (PersistentSnapshot loadedSnapshot = PersistentSnapshot.load(IntegerIDSerializer.INSTANCE, snapshotPath)) {
            assertThat(loadedSnapshot.getLastTerm()).isEqualTo(lastTerm);
            assertThat(loadedSnapshot.getLastIndex()).isEqualTo(lastIndex);
            assertThat(loadedSnapshot.getLastConfig()).usingRecursiveComparison().isEqualTo(lastConfig);
            byte[] contentBytesRead = new byte[9];
            loadedSnapshot.readInto(ByteBuffer.wrap(contentBytesRead), 0);
            assertThat(new String(contentBytesRead)).isEqualTo("Faster!!!");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private ConfigurationEntry createConfigurationEntry() {
        final Term configTerm = new Term(789);
        final Set<Integer> clusterMembers = Set.of(11, 22, 33, 44);
        return new ConfigurationEntry(configTerm, clusterMembers);
    }

    private PersistentSnapshot createAndPopulateSnapshot(Path snapshotPath, Term lastTerm, int lastIndex, ConfigurationEntry lastConfig, byte[] contentBytes) {
        try (final PersistentSnapshot persistentSnapshot = PersistentSnapshot.create(IntegerIDSerializer.INSTANCE, snapshotPath, lastIndex, lastTerm, lastConfig)) {
            persistentSnapshot.writeBytes(0, contentBytes);
            persistentSnapshot.finalise();
            return persistentSnapshot;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}