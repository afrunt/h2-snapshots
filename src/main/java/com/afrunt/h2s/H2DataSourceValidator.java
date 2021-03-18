package com.afrunt.h2s;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Objects;

/**
 * @author Andrii Frunt
 */
public class H2DataSourceValidator {
    public void validate(DataSource dataSource) {
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
}
