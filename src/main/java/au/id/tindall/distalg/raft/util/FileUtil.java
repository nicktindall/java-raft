package au.id.tindall.distalg.raft.util;

import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.Set;
import java.util.function.BiPredicate;

import static org.apache.logging.log4j.LogManager.getLogger;

public class FileUtil {

    private static final Logger LOGGER = getLogger();

    private static final Set<FileVisitOption> FVO = Collections.singleton(FileVisitOption.FOLLOW_LINKS);

    public static void deleteFilesMatching(Path path, int maxDepth, BiPredicate<Path, BasicFileAttributes> deletePredicate) {
        try {
            Files.walkFileTree(path, FVO, maxDepth, new RelentlessDeleter(deletePredicate));
        } catch (IOException e) {
            LOGGER.warn("Delete matching files failed", e);
        }
    }

    public static void deleteFileOrWarn(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            LOGGER.warn("Error deleting " + path, e);
        }
    }

    private static class RelentlessDeleter implements FileVisitor<Path> {

        private final BiPredicate<Path, BasicFileAttributes> deletePredicate;

        private RelentlessDeleter(BiPredicate<Path, BasicFileAttributes> deletePredicate) {
            this.deletePredicate = deletePredicate;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path path, BasicFileAttributes basicFileAttributes) {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path path, BasicFileAttributes basicFileAttributes) {
            if (deletePredicate.test(path, basicFileAttributes)) {
                deleteFileOrWarn(path);
            }
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path path, IOException e) {
            LOGGER.warn("Error visiting " + path, e);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path path, IOException e) {
            if (e != null) {
                LOGGER.warn("Error visiting " + path, e);
            }
            return FileVisitResult.CONTINUE;
        }
    }
}
