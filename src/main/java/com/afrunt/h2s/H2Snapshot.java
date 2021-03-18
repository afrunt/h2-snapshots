package com.afrunt.h2s;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Andrii Frunt
 */
public class H2Snapshot {
    private static final Logger LOGGER = LoggerFactory.getLogger(H2Snapshot.class);

    private final Path dirPath;
    private final Path dumpFilePath;

    public H2Snapshot(DataSource dataSource) {
        new H2DataSourceValidator().validate(dataSource);
        this.dirPath = Utils.createRandomTempDirectoryWithAutoDeletion();
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

    private void saveDump(DataSource dataSource) {
        execute(dataSource, String.format("SCRIPT TO 'file:%s'", dumpFilePath.toString()));
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

    private void execute(DataSource dataSource, String query) {
        try (Connection connection = dataSource.getConnection()) {
            Statement stat = connection.createStatement();
            stat.execute(query);
            stat.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
