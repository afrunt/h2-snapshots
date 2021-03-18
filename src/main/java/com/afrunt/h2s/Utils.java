package com.afrunt.h2s;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

/**
 * @author Andrii Frunt
 */
class Utils {
    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    public static boolean deleteDirectory(Path path) {
        return deleteDirectory(path.toFile());
    }

    public static boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    public static Path tempDirectoryPath() {
        return Path.of(System.getProperty("java.io.tmpdir"));
    }

    public static Path createRandomTempDirectoryWithAutoDeletion() {
        return createRandomTempDirectory(true);
    }

    public static Path createRandomTempDirectory(boolean autoDeletion) {
        Path tempDirectoryPath = tempDirectoryPath();

        Path dirPath;

        do {
            dirPath = tempDirectoryPath.resolve(UUID.randomUUID().toString());
        } while (dirPath.toFile().exists());

        try {
            Files.createDirectories(dirPath);
            if (autoDeletion) {
                final Path pathToDelete = dirPath;
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                            deleteDirectory(pathToDelete);
                            LOGGER.debug("Temporary directory {} deleted", pathToDelete.toAbsolutePath());
                        })
                );
            }
            return dirPath.toAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create directory " + dirPath.toAbsolutePath(), e);
        }
    }
}
