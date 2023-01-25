package au.id.tindall.distalg.raft.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class FileUtilTest {

    @Test
    void willDeleteFilesMatchingPredicate(@TempDir Path tempDir) throws IOException {
        final Path oneDeep = tempDir.resolve("one");
        final Path twoDeep = oneDeep.resolve("two");
        final Path threeDeep = twoDeep.resolve("three");
        Files.createDirectories(threeDeep);
        final Path keepOneDeep = Files.createFile(oneDeep.resolve("keep-oneDeep"));
        final Path deleteOneDeep = Files.createFile(oneDeep.resolve("delete-oneDeep"));
        final Path keepTwoDeep = Files.createFile(twoDeep.resolve("keep-twoDeep"));
        final Path deleteTwoDeep = Files.createFile(twoDeep.resolve("delete-twoDeep"));
        final Path keepThreeDeep = Files.createFile(threeDeep.resolve("keep-threeDeep"));
        final Path deleteThreeDeep = Files.createFile(threeDeep.resolve("delete-threeDeep"));

        FileUtil.deleteFilesMatching(oneDeep, 2,
                (p, attr) -> p.getFileName().toString().startsWith("delete"));

        assertThat(keepOneDeep).exists();
        assertThat(keepTwoDeep).exists();
        assertThat(keepThreeDeep).exists();

        assertThat(deleteOneDeep).doesNotExist();
        assertThat(deleteTwoDeep).doesNotExist();
        assertThat(deleteThreeDeep).exists();   // beyond depth limit
    }
}