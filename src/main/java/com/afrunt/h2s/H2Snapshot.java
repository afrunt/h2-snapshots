/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.afrunt.h2s;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.UUID;

/**
 * @author Andrii Frunt
 */
public class H2Snapshot {
    private static final Logger LOGGER = LoggerFactory.getLogger(H2Snapshot.class);

    private final Path dirPath;
    private final Path dumpFilePath;

    public H2Snapshot(DataSource dataSource) {
        validateDataSource(dataSource);
        this.dirPath = createRandomTempDirectoryWithAutoDeletion();
        this.dumpFilePath = dirPath.resolve("dump.sql").toAbsolutePath();
        final long started = System.currentTimeMillis();
        saveDump(dataSource);
        LOGGER.debug("Snapshot saved to {}. Elapsed {}ms", dumpFilePath, System.currentTimeMillis() - started);
    }

    public void apply(DataSource dataSource) {
        final long started = System.currentTimeMillis();
        execute(dataSource, "DROP ALL OBJECTS");
        execute(dataSource, String.format("RUNSCRIPT FROM 'file:%s'", dumpFilePath.toString()));
        LOGGER.debug("Database restored from snapshot {}. Elapsed {}ms", dumpFilePath, System.currentTimeMillis() - started);
    }

    public Path getDirPath() {
        return dirPath;
    }

    public Path getDumpFilePath() {
        return dumpFilePath;
    }

    public String getDumpFileContents() {
        try {
            return String.join("\n", Files.readAllLines(dumpFilePath));
        } catch (IOException e) {
            throw new RuntimeException("Unable to load the contents of dump file " + dumpFilePath, e);
        }
    }

    private void saveDump(DataSource dataSource) {
        execute(dataSource, String.format("SCRIPT TO 'file:%s'", dumpFilePath.toString()));
    }

    private void execute(DataSource dataSource, String query) {
        try (Connection connection = dataSource.getConnection()) {
            Statement stat = connection.createStatement();
            stat.execute(query);
            stat.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void validateDataSource(DataSource dataSource) {
        Objects.requireNonNull(dataSource);

        try (var connection = dataSource.getConnection()) {
            String databaseProductName = connection.getMetaData().getDatabaseProductName();
            if (!"H2".equalsIgnoreCase(databaseProductName)) {
                throw new IllegalArgumentException("Unknown type of the database " + databaseProductName);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean deleteDirectory(Path path) {
        return deleteDirectory(path.toFile());
    }

    private boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    private Path tempDirectoryPath() {
        return Path.of(System.getProperty("java.io.tmpdir"));
    }

    private Path createRandomTempDirectoryWithAutoDeletion() {
        return createRandomTempDirectory(true);
    }

    private Path createRandomTempDirectory(boolean autoDeletion) {
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
